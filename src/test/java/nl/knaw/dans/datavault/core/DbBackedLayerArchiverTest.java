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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class DbBackedLayerArchiverTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(LayerArchiveJob.class)
        .setProperty("hibernate.dialect", UuidAsCharH2Dialect.class.getName())
        .build();

    private LayerArchiveJobDao dao;
    private DbBackedLayerArchiver archiver;

    @BeforeEach
    public void setUp() {
        daoTestRule.getSessionFactory().getCurrentSession().beginTransaction();
        dao = new LayerArchiveJobDao(daoTestRule.getSessionFactory(), 2);
        archiver = new DbBackedLayerArchiver(dao);
    }

    @Test
    public void archive_registers_pending_job_without_running_the_action() {
        var count = new AtomicInteger();

        archiver.archive(1L, count::incrementAndGet);

        assertThat(count).hasValue(0);
        assertThat(dao.findByStatus(Status.PENDING)).singleElement().extracting(LayerArchiveJob::getLayerId).isEqualTo(1L);
    }

    @Test
    public void archive_twice_for_same_layer_registers_one_job() {
        archiver.archive(1L, () -> {
        });
        archiver.archive(1L, () -> {
        });

        assertThat(dao.list()).hasSize(1);
    }

    @Test
    public void rearchive_request_upgrades_existing_active_job_to_overwrite() {
        archiver.archive(1L, false, () -> {
        });
        archiver.archive(1L, true, () -> {
        });

        assertThat(dao.list()).singleElement().extracting(LayerArchiveJob::isOverwrite).isEqualTo(true);
    }

    @AfterEach
    public void tearDown() {
        daoTestRule.getSessionFactory().getCurrentSession().getTransaction().rollback();
    }
}
