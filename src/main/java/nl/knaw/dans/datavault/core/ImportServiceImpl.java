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
import nl.knaw.dans.datavault.api.ImportCommandDto;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ImportServiceImpl implements ImportService, Managed {
    // Must be a single thread executor to ensure that jobs are executed in the order they are submitted.
    private final ExecutorService jobExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setName("import-job-executor");
        return thread;
    });

    @NonNull
    private final ExecutorService createOrUpdateExecutor;

    @NonNull
    private final Pattern validObjectIdentifierPattern;

    @NonNull
    private final Path inboxDir;

    @NonNull
    private final Path outboxDir;

    @NonNull
    private final RepositoryProvider repositoryProvider;

    @NonNull
    private final LayerThresholdHandler layerThresholdHandler;

    private final List<ImportJob> importJobs = new ArrayList<>();

    @Builder
    public static ImportServiceImpl create(
        ExecutorService createOrUpdateExecutor,
        Pattern validObjectIdentifierPattern,
        Path inboxDir,
        Path outboxDir,
        RepositoryProvider repositoryProvider,
        LayerThresholdHandler layerThresholdHandler
    ) {
        return new ImportServiceImpl(
            createOrUpdateExecutor,
            validObjectIdentifierPattern,
            // Converting inbox and outbox to absolute paths. Normally, they should already be absolute, but in development testing, 
            // they are provided as paths relative to the project root for convenience.
            inboxDir.toAbsolutePath(),
            outboxDir.toAbsolutePath(),
            repositoryProvider,
            layerThresholdHandler);

    }

    @Override
    public ImportJob addImport(ImportCommandDto command) throws InvalidImportException {
        validateImportCommandDto(command);
        var batchOutbox = outboxDir.resolve(inboxDir.relativize(Path.of(command.getPath())));
        initializeBatchOutbox(batchOutbox);
        var importJob = ImportJob.builder()
            .path(Path.of(command.getPath()))
            .batchOutbox(batchOutbox)
            .singleObject(command.getSingleObject())
            .validObjectIdentifierPattern(validObjectIdentifierPattern)
            .executorService(createOrUpdateExecutor)
            .repositoryProvider(repositoryProvider)
            .layerThresholdHandler(layerThresholdHandler)
            .build();
        jobExecutor.execute(importJob);
        importJobs.add(importJob);
        return importJob;
    }

    private void initializeBatchOutbox(Path batchOutbox) {
        if (!Files.exists(batchOutbox)) {
            try {
                Files.createDirectories(batchOutbox.resolve("processed"));
                Files.createDirectories(batchOutbox.resolve("failed"));
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to create outbox directory '%s'", batchOutbox), e);
            }
        }
    }

    private void validateImportCommandDto(ImportCommandDto jobDto) throws InvalidImportException {
        var path = jobDto.getPath();
        if (!Path.of(path).isAbsolute()) {
            throw new InvalidImportException(String.format("Path '%s' is not an absolute path", path));
        }
        if (!Files.isDirectory(Path.of(path))) {
            throw new InvalidImportException(String.format("Path '%s' does not exist or is not a directory", path));
        }
        if (!Path.of(path).normalize().startsWith(inboxDir)) {
            throw new InvalidImportException(String.format("Path '%s' is not a subdirectory of the inbox directory %s", path, inboxDir));
        }
    }

    @Override
    public ImportJob getImport(UUID id) {
        return importJobs.stream()
            .filter(job -> job.getId().equals(id))
            .findFirst()
            .orElse(null);
    }

    @Override
    public void stop() {
        jobExecutor.shutdown();
    }
}
