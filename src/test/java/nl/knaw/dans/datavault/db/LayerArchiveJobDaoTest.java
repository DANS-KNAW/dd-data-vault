/*
 * Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.datavault.db;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.datavault.core.LayerArchiveJob;
import nl.knaw.dans.datavault.core.LayerArchiveJob.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.persistence.PersistenceException;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(DropwizardExtensionsSupport.class)
public class LayerArchiveJobDaoTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(LayerArchiveJob.class)
        .setProperty("hibernate.dialect", UuidAsCharH2Dialect.class.getName())
        .build();

    private LayerArchiveJobDao dao;

    @BeforeEach
    public void setUp() {
        daoTestRule.getSessionFactory().getCurrentSession().beginTransaction();
        dao = new LayerArchiveJobDao(daoTestRule.getSessionFactory(), 2);
    }

    @Test
    public void register_creates_pending_job() {
        var job = dao.register(1L, false);

        assertThat(job.getStatus()).isEqualTo(Status.PENDING);
        assertThat(job.getActiveLayerId()).isEqualTo(1L);
        assertThat(dao.find(job.getId())).get().satisfies(found -> {
            assertThat(found.getLayerId()).isEqualTo(1L);
            assertThat(found.getActiveLayerId()).isEqualTo(1L);
        });
    }

    @Test
    public void register_returns_existing_unfinished_job_for_same_layer() {
        var first = dao.register(1L, false);
        var second = dao.register(1L, false);

        assertThat(second.getId()).isEqualTo(first.getId());
        assertThat(dao.list()).hasSize(1);
    }

    @Test
    public void register_creates_new_job_when_previous_one_has_finished() {
        var first = dao.register(1L, false);
        dao.finish(first.getId(), Status.FAILED, "oops");

        var second = dao.register(1L, false);

        assertThat(second.getId()).isNotEqualTo(first.getId());
        assertThat(dao.find(first.getId())).get().extracting(LayerArchiveJob::getMessage).isEqualTo("oops");
    }

    @Test
    public void find_latest_by_layer_returns_empty_when_layer_has_no_jobs() {
        dao.register(2L, false);

        assertThat(dao.findLatestByLayer(1L)).isEmpty();
    }

    @Test
    public void find_latest_by_layer_selects_newest_job_including_unfinished_retry() {
        var failed = dao.register(1L, false);
        failed.setCreated(OffsetDateTime.parse("2024-01-01T00:00:00Z"));
        dao.finish(failed.getId(), Status.FAILED, "failed");
        var retry = dao.register(1L, false);
        retry.setCreated(OffsetDateTime.parse("2024-01-02T00:00:00Z"));
        var unrelated = dao.register(2L, false);
        unrelated.setCreated(OffsetDateTime.parse("2024-01-03T00:00:00Z"));

        var session = daoTestRule.getSessionFactory().getCurrentSession();
        session.flush();
        session.clear();

        assertThat(dao.findLatestByLayer(1L)).get().satisfies(job -> {
            assertThat(job.getId()).isEqualTo(retry.getId());
            assertThat(job.getStatus()).isEqualTo(Status.PENDING);
        });
        assertThat(dao.findLatestByLayer(2L)).get().extracting(LayerArchiveJob::getId).isEqualTo(unrelated.getId());
    }

    @Test
    public void find_latest_by_layer_breaks_creation_time_ties_by_descending_id() {
        var created = OffsetDateTime.parse("2024-01-01T00:00:00Z");
        var first = dao.register(1L, false);
        first.setCreated(created);
        dao.finish(first.getId(), Status.FAILED, "failed");
        var second = dao.register(1L, false);
        second.setCreated(created);
        var expectedId = first.getId().toString().compareTo(second.getId().toString()) > 0 ? first.getId() : second.getId();

        var session = daoTestRule.getSessionFactory().getCurrentSession();
        session.flush();
        session.clear();

        assertThat(dao.findLatestByLayer(1L)).get().extracting(LayerArchiveJob::getId).isEqualTo(expectedId);
    }

    @Test
    public void finish_clears_active_layer_id_for_success_and_failed() {
        var first = dao.register(1L, false);
        dao.finish(first.getId(), Status.SUCCESS, "done");

        var finishedSuccess = dao.find(first.getId()).orElseThrow();
        assertThat(finishedSuccess.getStatus()).isEqualTo(Status.SUCCESS);
        assertThat(finishedSuccess.getActiveLayerId()).isNull();

        var second = dao.register(1L, false);
        assertThat(second.getActiveLayerId()).isEqualTo(1L);
        dao.finish(second.getId(), Status.FAILED, "error");

        var finishedFailed = dao.find(second.getId()).orElseThrow();
        assertThat(finishedFailed.getStatus()).isEqualTo(Status.FAILED);
        assertThat(finishedFailed.getActiveLayerId()).isNull();
    }

    @Test
    public void nextInputs_claims_oldest_pending_jobs_up_to_the_parallelism_limit() {
        var j1 = dao.register(1L, false);
        var j2 = dao.register(2L, false);
        var j3 = dao.register(3L, false);

        var claimed = dao.nextInputs();

        assertThat(claimed).extracting(LayerArchiveJob::getId).containsExactly(j1.getId(), j2.getId());
        assertThat(dao.findByStatus(Status.RUNNING)).hasSize(2);
        assertThat(dao.nextInputs()).isEmpty();

        dao.finish(j1.getId(), Status.SUCCESS, null);

        assertThat(dao.nextInputs()).extracting(LayerArchiveJob::getId).containsExactly(j3.getId());
    }

    @Test
    public void nextInputs_counts_attempts_and_requeue_does_not() {
        var job = dao.register(1L, false);

        assertThat(dao.nextInputs()).singleElement().extracting(LayerArchiveJob::getAttempts).isEqualTo(1);
        dao.requeue(job.getId(), "moratorium");

        var requeued = dao.find(job.getId()).orElseThrow();
        assertThat(requeued.getStatus()).isEqualTo(Status.PENDING);
        assertThat(requeued.getActiveLayerId()).isEqualTo(1L);
        assertThat(requeued.getAttempts()).isZero();
        assertThat(requeued.getMessage()).isEqualTo("moratorium");
    }

    @Test
    public void requeue_maintains_active_layer_id() {
        var job = dao.register(1L, false);
        dao.nextInputs();

        var running = dao.find(job.getId()).orElseThrow();
        assertThat(running.getStatus()).isEqualTo(Status.RUNNING);
        assertThat(running.getActiveLayerId()).isEqualTo(1L);

        dao.requeue(job.getId(), "moratorium");

        var requeued = dao.find(job.getId()).orElseThrow();
        assertThat(requeued.getStatus()).isEqualTo(Status.PENDING);
        assertThat(requeued.getActiveLayerId()).isEqualTo(1L);
    }

    @Test
    public void resetInterruptedJobs_makes_running_jobs_pending_again() {
        dao.register(1L, false);
        dao.register(2L, false);
        dao.nextInputs();
        var done = dao.register(3L, false);
        dao.finish(done.getId(), Status.SUCCESS, null);

        dao.resetInterruptedJobs();

        assertThat(dao.findByStatus(Status.PENDING)).extracting(LayerArchiveJob::getLayerId).containsExactly(1L, 2L);
        assertThat(dao.findByStatus(Status.PENDING)).extracting(LayerArchiveJob::getActiveLayerId).containsExactly(1L, 2L);
        assertThat(dao.findByStatus(Status.RUNNING)).isEmpty();
    }

    @Test
    public void persisting_duplicate_active_jobs_for_same_layer_fails_unique_constraint() {
        var session = daoTestRule.getSessionFactory().getCurrentSession();

        var job1 = LayerArchiveJob.builder()
            .layerId(10L)
            .activeLayerId(10L)
            .created(OffsetDateTime.now())
            .status(Status.PENDING)
            .build();
        session.save(job1);
        session.flush();

        var job2 = LayerArchiveJob.builder()
            .layerId(10L)
            .activeLayerId(10L)
            .created(OffsetDateTime.now())
            .status(Status.PENDING)
            .build();
        session.save(job2);

        assertThatThrownBy(session::flush)
            .isInstanceOf(PersistenceException.class);
    }

    @Test
    public void multiple_finished_jobs_for_same_layer_can_coexist_with_null_active_layer_id() {
        var j1 = dao.register(1L, false);
        dao.finish(j1.getId(), Status.SUCCESS, "archived");

        var j2 = dao.register(1L, false);
        dao.finish(j2.getId(), Status.FAILED, "failed");

        var j3 = dao.register(1L, false);

        var session = daoTestRule.getSessionFactory().getCurrentSession();
        session.flush();

        var allJobs = dao.list();
        assertThat(allJobs).hasSize(3);
        assertThat(dao.find(j1.getId())).get().extracting(LayerArchiveJob::getActiveLayerId).isNull();
        assertThat(dao.find(j2.getId())).get().extracting(LayerArchiveJob::getActiveLayerId).isNull();
        assertThat(dao.find(j3.getId())).get().extracting(LayerArchiveJob::getActiveLayerId).isEqualTo(1L);

        var registeredAgain = dao.register(1L, true);
        assertThat(registeredAgain.getId()).isEqualTo(j3.getId());
        assertThat(registeredAgain.isOverwrite()).isTrue();
    }

    @AfterEach
    public void tearDown() {
        daoTestRule.getSessionFactory().getCurrentSession().getTransaction().rollback();
    }
}
