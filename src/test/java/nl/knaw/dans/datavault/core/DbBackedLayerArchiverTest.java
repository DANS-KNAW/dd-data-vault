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
package nl.knaw.dans.datavault.core;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.datavault.core.LayerArchiveJob.Status;
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.datavault.db.UuidAsCharH2Dialect;
import nl.knaw.dans.layerstore.ItemsMismatchException;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DropwizardExtensionsSupport.class)
public class DbBackedLayerArchiverTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(LayerArchiveJob.class)
        .setProperty("hibernate.dialect", UuidAsCharH2Dialect.class.getName())
        .build();

    private LayerArchiveJobDao dao;
    private LayerConsistencyChecker checker;
    private DbBackedLayerArchiver archiver;

    @BeforeEach
    public void setUp() {
        daoTestRule.getSessionFactory().getCurrentSession().beginTransaction();
        dao = new LayerArchiveJobDao(daoTestRule.getSessionFactory(), 2);
        checker = mock(LayerConsistencyChecker.class);
        archiver = new DbBackedLayerArchiver(dao, checker);
    }

    private void runNextJob() {
        var job = dao.nextInputs().get(0);
        archiver.taskFactory().create(job).run();
    }

    private Status statusOf(long layerId) {
        return dao.list().stream().filter(j -> j.getLayerId() == layerId).reduce((a, b) -> b).orElseThrow().getStatus();
    }

    @Test
    public void archive_registers_job_without_running_the_action() {
        var count = new AtomicInteger();

        archiver.archive(1L, count::incrementAndGet);

        assertThat(count).hasValue(0);
        assertThat(statusOf(1L)).isEqualTo(Status.PENDING);
    }

    @Test
    public void job_runs_consistency_check_and_action_and_ends_in_success() throws Exception {
        var count = new AtomicInteger();
        archiver.archive(1L, count::incrementAndGet);

        runNextJob();

        verify(checker).check(1L);
        assertThat(count).hasValue(1);
        assertThat(statusOf(1L)).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void job_fails_when_consistency_check_fails_and_action_is_not_run() throws Exception {
        var count = new AtomicInteger();
        doThrow(new ItemsMismatchException(List.of(), List.of())).when(checker).check(1L);
        archiver.archive(1L, count::incrementAndGet);

        runNextJob();

        assertThat(count).hasValue(0);
        assertThat(statusOf(1L)).isEqualTo(Status.FAILED);
        assertThat(dao.list().get(0).getMessage()).contains("ItemsMismatchException");
    }

    @Test
    public void job_fails_when_action_throws() {
        archiver.archive(1L, () -> {
            throw new IllegalStateException("boom");
        });

        runNextJob();

        assertThat(statusOf(1L)).isEqualTo(Status.FAILED);
        assertThat(dao.list().get(0).getMessage()).contains("boom");
    }

    @Test
    public void job_stays_pending_when_moratorium_is_in_effect() {
        archiver.archive(1L, () -> {
            throw new MoratoriumException(Instant.now());
        });

        runNextJob();

        assertThat(statusOf(1L)).isEqualTo(Status.PENDING);
    }

    @Test
    public void unfinished_jobs_are_resumed_by_requesting_archiving_again() {
        archiver.archive(1L, () -> {
        });
        dao.nextInputs(); // job is RUNNING, as if the service stopped now
        var restarted = new DbBackedLayerArchiver(dao, checker);
        var store = mock(LayeredItemStore.class);
        var count = new AtomicInteger();
        doAnswer(inv -> {
            restarted.archive(inv.getArgument(0), count::incrementAndGet);
            return null;
        }).when(store).archiveLayer(anyLong(), anyBoolean());

        restarted.resumeUnfinishedJobs(store);

        verify(store).archiveLayer(1L, true);
        assertThat(dao.list()).hasSize(1);
        assertThat(statusOf(1L)).isEqualTo(Status.PENDING);
        var job = dao.nextInputs().get(0);
        restarted.taskFactory().create(job).run();
        assertThat(count).hasValue(1);
        assertThat(statusOf(1L)).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void resume_marks_job_success_when_layer_was_already_archived() {
        archiver.archive(1L, () -> {
        });
        dao.nextInputs();
        var store = mock(LayeredItemStore.class);
        doThrow(new IllegalStateException("Layer 1 is in state ARCHIVED, but must be in state CLOSED for this operation")).when(store).archiveLayer(1L, true);

        archiver.resumeUnfinishedJobs(store);

        assertThat(statusOf(1L)).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void resume_marks_job_failed_when_layer_cannot_be_archived() {
        archiver.archive(1L, () -> {
        });
        dao.nextInputs();
        var store = mock(LayeredItemStore.class);
        doThrow(new IllegalArgumentException("no such layer")).when(store).archiveLayer(1L, true);

        archiver.resumeUnfinishedJobs(store);

        assertThat(statusOf(1L)).isEqualTo(Status.FAILED);
    }

    @AfterEach
    public void tearDown() {
        daoTestRule.getSessionFactory().getCurrentSession().getTransaction().rollback();
    }
}
