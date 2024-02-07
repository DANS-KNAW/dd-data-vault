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

import lombok.AllArgsConstructor;
import lombok.Builder;
import nl.knaw.dans.datavault.db.JobDao;

import java.util.concurrent.ExecutorService;

@Builder
public class JobRunner implements JobHandler {
    private JobDao jobDao;
    private String validObjectIdentifierPattern;
    private ExecutorService executorService;
    private RepositoryProvider repositoryProvider;

    @Override
    public void handleJob(Job job) {
        jobDao.updateState(job.getId(), Job.State.RUNNING);
        try {
            JobTask.builder()
                .batchDirectory(job.getBatch())
                .validObjectIdentifierPattern(validObjectIdentifierPattern)
                .executorService(executorService)
                .repositoryProvider(repositoryProvider)
                .build().run();
            jobDao.updateState(job.getId(), Job.State.SUCCESS);
        }
        catch (Exception e) {
            jobDao.updateState(job.getId(), Job.State.FAIL);
            throw e;
        }
    }
}
