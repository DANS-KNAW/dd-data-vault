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
import lombok.SneakyThrows;
import nl.knaw.dans.datavault.core.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class JobDaoTest {

    protected final DAOTestExtension daoTestExtension = DAOTestExtension.newBuilder()
        .addEntityClass(Job.class)
        .build();
    private final JobDao jobDao = new JobDao(daoTestExtension.getSessionFactory());

    @Test
    public void create_should_persist_a_job() {
        Job job = Job.builder()
            .batch(null)
            .build();
        jobDao.create(job);

        daoTestExtension.inTransaction(() -> {
            var jobs = jobDao.findAll();
            assertThat(jobs.get(0).getBatch()).isEqualTo(job.getBatch());
        });
    }

    @Test
    public void getNextJobs_should_only_return_nonfinished_jobs_up_to_limited_number() throws Exception {
        var job1 = Job.builder()
            .batch(Path.of("some/path/to/batch"))
            .build();
        var job2 = Job.builder()
            .batch(Path.of("some/other/path/to/batch"))
            .build();
        var job3 = Job.builder()
            .batch(Path.of("yet/another/path/to/batch"))
            .build();
        daoTestExtension.inTransaction(() -> {
            jobDao.create(job1);
            // wait a millisecond to ensure that the creationTimestamps are different
            sleep(2);
            jobDao.create(job2);
            sleep(2);
            jobDao.create(job3);
        });
        daoTestExtension.inTransaction(() -> {
            var jobs = jobDao.getNextJobs(2);
            assertThat(jobs).asList().containsExactly(job1, job2);
        });
    }

    @Test
    public void getNextJobs_should_return_all_nonfinished_jobs_if_number_is_larger_than_number_of_nonfinished_jobs() throws Exception {
        var job1 = Job.builder()
            .batch(Path.of("some/path/to/batch"))
            .build();
        var job2 = Job.builder()
            .batch(Path.of("some/other/path/to/batch"))
            .build();
        var job3 = Job.builder()
            .batch(Path.of("yet/another/path/to/batch"))
            .build();
        daoTestExtension.inTransaction(() -> {
            jobDao.create(job1);
            // wait a millisecond to ensure that the creationTimestamps are different
            sleep(2);
            jobDao.create(job2);
            sleep(2);
            jobDao.create(job3);
        });
        daoTestExtension.inTransaction(() -> {
            var jobs = jobDao.getNextJobs(5);
            assertThat(jobs).asList().containsExactly(job1, job2, job3);
        });
    }

    @Test
    public void getNextJobs_should_return_empty_list_if_no_nonfinished_jobs() {
        var job1 = Job.builder()
            .batch(Path.of("some/path/to/batch"))
            .finished(true)
            .build();
        var job2 = Job.builder()
            .batch(Path.of("some/other/path/to/batch"))
            .finished(true)
            .build();
        daoTestExtension.inTransaction(() -> {
            jobDao.create(job1);
            sleep(2);
            jobDao.create(job2);
        });

        daoTestExtension.inTransaction(() -> {
            var jobs = jobDao.getNextJobs(5);
            assertThat(jobs).asList().isEmpty();
        });
    }

    @Test
    public void getNextJobs_should_not_return_finished_jobs() {
        var job1 = Job.builder()
            .batch(Path.of("some/path/to/batch"))
            .build();
        var job2 = Job.builder()
            .batch(Path.of("some/other/path/to/batch"))
            .finished(true)
            .build();
        var job3 = Job.builder()
            .batch(Path.of("yet/another/path/to/batch"))
            .build();
        daoTestExtension.inTransaction(() -> {
            jobDao.create(job1);
            sleep(2);
            jobDao.create(job2);
            sleep(2);
            jobDao.create(job3);
        });
        daoTestExtension.inTransaction(() -> {
            var jobs = jobDao.getNextJobs(5);
            assertThat(jobs).asList().containsExactly(job1, job3);
        });
    }

    @SneakyThrows
    private void sleep(int i) {
        Thread.sleep(i);
    }

}
