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
import java.util.Set;

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

    @Test
    public void list_filtered_should_apply_status_and_exact_path_filters() {
        var now = OffsetDateTime.now();
        var first = createJob("batch", ImportJob.Status.SUCCESS, now.minusHours(1));
        var second = createJob("batch", ImportJob.Status.FAILED, now);
        createJob("batch", ImportJob.Status.PENDING, now);
        createJob("batch-extra", ImportJob.Status.SUCCESS, now);

        var jobs = daoTestRule.inTransaction(() -> dao.listFiltered(
            Set.of(ImportJob.Status.SUCCESS, ImportJob.Status.FAILED), "batch", null, null));
        assertThat(jobs).extracting(ImportJob::getId).containsExactly(first.getId(), second.getId());
        assertThat(daoTestRule.inTransaction(() -> dao.listFiltered(Set.of(ImportJob.Status.RUNNING), "batch", null, null))).isEmpty();
    }

    @Test
    public void list_filtered_should_page_in_stable_created_and_id_order() {
        var now = OffsetDateTime.now();
        var early = createJob("early", ImportJob.Status.PENDING, now.minusHours(1));
        createJob("same", ImportJob.Status.SUCCESS, now);
        createJob("same", ImportJob.Status.FAILED, now);
        var late = createJob("late", ImportJob.Status.RUNNING, now.plusHours(1));

        var all = daoTestRule.inTransaction(() -> dao.listFiltered(null, null, null, null));
        assertThat(all).hasSize(4);
        assertThat(all.get(0).getId()).isEqualTo(early.getId());
        assertThat(all.get(3).getId()).isEqualTo(late.getId());
        assertThat(all.get(1).getId().toString()).isLessThan(all.get(2).getId().toString());
        var page = daoTestRule.inTransaction(() -> dao.listFiltered(Set.of(), null, 2, 1));
        assertThat(page).extracting(ImportJob::getId)
            .containsExactly(all.get(1).getId(), all.get(2).getId());
        assertThat(daoTestRule.inTransaction(() -> dao.listFiltered(null, null, 2, 4))).isEmpty();
    }

    private ImportJob createJob(String path, ImportJob.Status status, OffsetDateTime created) {
        return daoTestRule.inTransaction(() -> dao.create(ImportJob.builder()
            .path(path)
            .status(status)
            .created(created)
            .build()));
    }
}
