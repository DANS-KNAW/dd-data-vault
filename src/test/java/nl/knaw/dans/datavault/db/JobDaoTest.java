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

    @SneakyThrows
    private void sleep(int i) {
        Thread.sleep(i);
    }

}
