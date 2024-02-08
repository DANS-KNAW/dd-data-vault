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

import io.dropwizard.lifecycle.Managed;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.datavault.api.JobDto;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class JobServiceImpl implements JobService, Managed {
    // Must be a single thread executor to ensure that jobs are executed in the order they are submitted.
    private final ExecutorService jobExecutor = Executors.newSingleThreadExecutor();

    @NonNull
    private final ExecutorService createOrUpdateExecutor;

    @NonNull
    private final Pattern validObjectIdentifierPattern;

    @NonNull
    private final RepositoryProvider repositoryProvider;

    @Builder
    public static JobServiceImpl create(ExecutorService createOrUpdateExecutor, Pattern validObjectIdentifierPattern, RepositoryProvider repositoryProvider) {
        return new JobServiceImpl(createOrUpdateExecutor, validObjectIdentifierPattern, repositoryProvider);
    }

    @Override
    public void startJob(JobDto jobDto) throws InvalidJobException {
        validateJobDto(jobDto);
        jobExecutor.execute(JobTask.builder()
            .batchDirectory(Path.of(jobDto.getBatch()))
            .validObjectIdentifierPattern(validObjectIdentifierPattern)
            .executorService(createOrUpdateExecutor)
            .repositoryProvider(repositoryProvider)
            .build());
    }

    private void validateJobDto(JobDto jobDto) throws InvalidJobException {
        var directoryName = jobDto.getBatch();
        if (!Files.isDirectory(Path.of(directoryName))) {
            throw new InvalidJobException(String.format("Batch directory '%s' does not exist or is not a directory", directoryName));
        }
    }

    @Override
    public void stop() {
        jobExecutor.shutdown();
    }
}
