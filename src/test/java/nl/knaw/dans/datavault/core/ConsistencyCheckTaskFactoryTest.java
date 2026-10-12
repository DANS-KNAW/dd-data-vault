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

import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.datavault.core.ConsistencyCheck.Result;
import nl.knaw.dans.datavault.core.ConsistencyCheck.Type;
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import nl.knaw.dans.layerstore.ItemsMismatchException;
import nl.knaw.dans.layerstore.LayeredItemStore;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.context.internal.ManagedSessionContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.OffsetDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ConsistencyCheckTaskFactoryTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(ConsistencyCheck.class)
        .build();

    private ConsistencyCheckDao dao;
    private LayeredItemStore layeredItemStore;
    private ConsistencyCheckTaskFactory factory;
    private Session testSession;

    @BeforeEach
    public void setUp() {
        // Hibernate 5 maps UUIDs to BINARY(255) on H2, which is right-padded, so that lookups by id never match. Use VARBINARY instead.
        daoTestRule.inTransaction(() -> daoTestRule.getSessionFactory().getCurrentSession()
            .doWork(connection -> {
                try (var statement = connection.createStatement()) {
                    statement.execute("ALTER TABLE consistency_check ALTER COLUMN id SET DATA TYPE VARBINARY(255)");
                }
            }));
        dao = new ConsistencyCheckDao(daoTestRule.getSessionFactory());
        layeredItemStore = mock(LayeredItemStore.class);
        factory = new ConsistencyCheckTaskFactory(
            new UnitOfWorkAwareProxyFactory("hibernate", daoTestRule.getSessionFactory()), dao, layeredItemStore);
    }

    @AfterEach
    public void tearDown() {
        // Rebind the session opened by DAOTestExtension, so that it can clean up properly
        if (testSession != null && !ManagedSessionContext.hasBind(daoTestRule.getSessionFactory())) {
            ManagedSessionContext.bind(testSession);
        }
    }

    private ConsistencyCheck createCheck(Type type, long layerId) {
        var check = new ConsistencyCheck();
        check.setType(type);
        check.setLayerId(layerId);
        check.setCreated(OffsetDateTime.now().minusMinutes(1));
        return daoTestRule.inTransaction(() -> dao.create(check));
    }

    /**
     * Simulates the situation in the PollingTaskExecutor, where the task is run on a thread without a session bound to it.
     */
    private void unbindTestSession() {
        testSession = ManagedSessionContext.unbind(daoTestRule.getSessionFactory());
        assertThat(testSession).isNotNull();
    }

    private ConsistencyCheck reload(ConsistencyCheck check) {
        try (var session = daoTestRule.getSessionFactory().openSession()) {
            return session.get(ConsistencyCheck.class, check.getId());
        }
    }

    @Test
    public void task_created_without_proxy_fails_when_no_session_is_bound() {
        var check = createCheck(Type.LISTING_RECORDS, 1L);
        unbindTestSession();

        var task = new ConsistencyCheckTask(dao, check, layeredItemStore);

        assertThatThrownBy(task::run)
            .isInstanceOf(HibernateException.class)
            .hasMessageContaining("No session currently bound");
    }

    @Test
    public void create_should_return_task_that_runs_in_its_own_unit_of_work() throws Exception {
        var check = createCheck(Type.LISTING_RECORDS, 1L);
        unbindTestSession();

        factory.create(check).run();

        verify(layeredItemStore).checkLayerItemRecords(1L);
        var reloaded = reload(check);
        assertThat(reloaded.getStarted()).isNotNull();
        assertThat(reloaded.getFinished()).isNotNull();
        assertThat(reloaded.getResult()).isEqualTo(Result.OK);
        assertThat(reloaded.getMessage()).isNull();
        assertThat(ManagedSessionContext.hasBind(daoTestRule.getSessionFactory())).isFalse();
    }

    @Test
    public void create_should_return_task_that_records_mismatch_as_not_ok() throws Exception {
        var check = createCheck(Type.LISTING_RECORDS, 2L);
        doThrow(new ItemsMismatchException(List.of(), List.of())).when(layeredItemStore).checkLayerItemRecords(2L);
        unbindTestSession();

        factory.create(check).run();

        var reloaded = reload(check);
        assertThat(reloaded.getFinished()).isNotNull();
        assertThat(reloaded.getResult()).isEqualTo(Result.NOT_OK);
    }

    @Test
    public void create_should_return_task_that_checks_layer_ids() throws Exception {
        var check = createCheck(Type.LAYER_IDS, 0L);
        unbindTestSession();

        factory.create(check).run();

        verify(layeredItemStore).checkSameLayersOnStorageAndDb();
        var reloaded = reload(check);
        assertThat(reloaded.getResult()).isEqualTo(Result.OK);
    }
}
