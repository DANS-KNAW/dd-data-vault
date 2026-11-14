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

import static org.assertj.core.api.Assertions.assertThat;

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
        var job = dao.register(1L);

        assertThat(job.getStatus()).isEqualTo(Status.PENDING);
        assertThat(dao.find(job.getId())).get().extracting(LayerArchiveJob::getLayerId).isEqualTo(1L);
    }

    @Test
    public void register_returns_existing_unfinished_job_for_same_layer() {
        var first = dao.register(1L);
        var second = dao.register(1L);

        assertThat(second.getId()).isEqualTo(first.getId());
        assertThat(dao.list()).hasSize(1);
    }

    @Test
    public void register_creates_new_job_when_previous_one_has_finished() {
        var first = dao.register(1L);
        dao.finish(first.getId(), Status.FAILED, "oops");

        var second = dao.register(1L);

        assertThat(second.getId()).isNotEqualTo(first.getId());
        assertThat(dao.find(first.getId())).get().extracting(LayerArchiveJob::getMessage).isEqualTo("oops");
    }

    @Test
    public void nextInputs_claims_oldest_pending_jobs_up_to_the_parallelism_limit() {
        var j1 = dao.register(1L);
        var j2 = dao.register(2L);
        var j3 = dao.register(3L);

        var claimed = dao.nextInputs();

        assertThat(claimed).extracting(LayerArchiveJob::getId).containsExactly(j1.getId(), j2.getId());
        assertThat(dao.findByStatus(Status.RUNNING)).hasSize(2);
        assertThat(dao.nextInputs()).isEmpty();

        dao.finish(j1.getId(), Status.SUCCESS, null);

        assertThat(dao.nextInputs()).extracting(LayerArchiveJob::getId).containsExactly(j3.getId());
    }

    @Test
    public void nextInputs_counts_attempts_and_requeue_does_not() {
        var job = dao.register(1L);

        assertThat(dao.nextInputs()).singleElement().extracting(LayerArchiveJob::getAttempts).isEqualTo(1);
        dao.requeue(job.getId(), "moratorium");

        var requeued = dao.find(job.getId()).orElseThrow();
        assertThat(requeued.getStatus()).isEqualTo(Status.PENDING);
        assertThat(requeued.getAttempts()).isZero();
        assertThat(requeued.getMessage()).isEqualTo("moratorium");
    }

    @Test
    public void resetInterruptedJobs_makes_running_jobs_pending_again() {
        dao.register(1L);
        dao.register(2L);
        dao.nextInputs();
        var done = dao.register(3L);
        dao.finish(done.getId(), Status.SUCCESS, null);

        var pending = dao.resetInterruptedJobs();

        assertThat(pending).extracting(LayerArchiveJob::getLayerId).containsExactly(1L, 2L);
        assertThat(dao.findByStatus(Status.RUNNING)).isEmpty();
    }

    @AfterEach
    public void tearDown() {
        daoTestRule.getSessionFactory().getCurrentSession().getTransaction().rollback();
    }
}
