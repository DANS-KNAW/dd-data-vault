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
import nl.knaw.dans.datavault.core.ImportJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ImportJobDaoTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(ImportJob.class)
        .build();

    private ImportJobDao dao;

    @BeforeEach
    public void setUp() {
        dao = new ImportJobDao(daoTestRule.getSessionFactory());
    }

    @Test
    public void findByStatus_should_return_jobs_matching_status_ordered_by_created() {
        var now = OffsetDateTime.now();

        var job1 = daoTestRule.inTransaction(() -> dao.create(ImportJob.builder()
            .path("batch1")
            .singleObject(false)
            .status(ImportJob.Status.SUCCESS)
            .created(now.minusHours(2))
            .build()));

        daoTestRule.inTransaction(() -> dao.create(ImportJob.builder()
            .path("batch2")
            .singleObject(false)
            .status(ImportJob.Status.FAILED)
            .created(now.minusHours(1))
            .build()));

        var job3 = daoTestRule.inTransaction(() -> dao.create(ImportJob.builder()
            .path("batch3")
            .singleObject(false)
            .status(ImportJob.Status.SUCCESS)
            .created(now)
            .build()));

        var successfulJobs = daoTestRule.inTransaction(() -> dao.findByStatus(ImportJob.Status.SUCCESS));
        assertThat(successfulJobs)
            .extracting(ImportJob::getId)
            .containsExactly(job1.getId(), job3.getId());
    }
}
