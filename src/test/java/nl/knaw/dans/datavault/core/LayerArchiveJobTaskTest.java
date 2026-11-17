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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
public class LayerArchiveJobTaskTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(LayerArchiveJob.class)
        .setProperty("hibernate.dialect", UuidAsCharH2Dialect.class.getName())
        .build();

    private LayerArchiveJobDao dao;
    private LayerConsistencyChecker checker;
    private LayeredItemStore store;
    private LayerArchiveJobTaskFactory factory;

    @BeforeEach
    public void setUp() {
        daoTestRule.getSessionFactory().getCurrentSession().beginTransaction();
        dao = new LayerArchiveJobDao(daoTestRule.getSessionFactory(), 2);
        checker = mock(LayerConsistencyChecker.class);
        store = mock(LayeredItemStore.class);
        factory = new LayerArchiveJobTaskFactory(dao, store, checker);
    }

    private void registerAndRun() {
        dao.register(1L, false);
        factory.create(dao.nextInputs().get(0)).run();
    }

    private LayerArchiveJob theJob() {
        return dao.list().get(0);
    }

    @Test
    public void job_runs_consistency_check_and_archiving_and_ends_in_success() throws Exception {
        registerAndRun();

        verify(checker).check(1L);
        verify(store).archiveLayerDirectly(1L, false);
        assertThat(theJob().getStatus()).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void job_fails_when_consistency_check_fails_and_layer_is_not_archived() throws Exception {
        doThrow(new ItemsMismatchException(List.of(), List.of())).when(checker).check(1L);

        registerAndRun();

        verify(store, never()).archiveLayerDirectly(anyLong(), anyBoolean());
        assertThat(theJob().getStatus()).isEqualTo(Status.FAILED);
        assertThat(theJob().getMessage()).contains("ItemsMismatchException");
    }

    @Test
    public void job_fails_when_archiving_throws() {
        doThrow(new IllegalStateException("boom")).when(store).archiveLayerDirectly(1L, false);

        registerAndRun();

        assertThat(theJob().getStatus()).isEqualTo(Status.FAILED);
        assertThat(theJob().getMessage()).contains("boom");
    }

    @Test
    public void job_is_requeued_when_moratorium_is_in_effect() {
        doThrow(new MoratoriumException(Instant.now())).when(store).archiveLayerDirectly(1L, false);

        registerAndRun();

        assertThat(theJob().getStatus()).isEqualTo(Status.PENDING);
    }

    @Test
    public void job_succeeds_without_archiving_when_layer_was_already_archived() throws Exception {
        when(store.isLayerArchived(1L)).thenReturn(true);

        registerAndRun();

        verify(store, never()).archiveLayerDirectly(anyLong(), anyBoolean());
        assertThat(theJob().getStatus()).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void interrupted_job_is_picked_up_and_executed_after_restart() throws Exception {
        dao.register(1L, false);
        dao.nextInputs(); // job is RUNNING, as if the service stopped now

        dao.resetInterruptedJobs();
        factory.create(dao.nextInputs().get(0)).run();

        verify(store).archiveLayerDirectly(1L, true);
        assertThat(dao.list()).hasSize(1);
        assertThat(theJob().getStatus()).isEqualTo(Status.SUCCESS);
    }

    @Test
    public void rearchive_request_overwrites_existing_archive_on_first_attempt() {
        dao.register(1L, true);

        factory.create(dao.nextInputs().get(0)).run();

        verify(store).archiveLayerDirectly(1L, true);
    }

    @AfterEach
    public void tearDown() {
        daoTestRule.getSessionFactory().getCurrentSession().getTransaction().rollback();
    }
}
