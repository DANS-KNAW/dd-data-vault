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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * Represents an import job that processes a batch directory containing object import directories. It is also possible to process a single object import directory (which must then still be placed in a
 * batch directory).
 */
@Builder(builderClassName = "Builder")
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class ImportTask implements Runnable {
    public enum Status {
        PENDING,
        RUNNING,
        SUCCESS,
        FAILED
    }

    @Data
    private static class ObjectValidationResult {
        private boolean objectImportDirNameIsValid;
        private final List<Path> invalidVersionDirectories = new ArrayList<>();
    }

    @Default
    private final Pattern validObjectIdentifierPattern = Pattern.compile(".+");
    @NonNull
    private final ExecutorService executorService;
    @NonNull
    private final RepositoryProvider repositoryProvider;

    @NonNull
    private final LayerThresholdHandler layerThresholdHandler;

    private final boolean acceptTimestampVersionDirectories;

    @Default
    @Getter
    private final UUID id = UUID.randomUUID();

    @Getter
    @NonNull
    private final Path path;

    @NonNull
    private final Path batchOutbox;

    @Getter
    private final boolean singleObject;

    @Default
    @Getter
    private Status status = Status.PENDING;

    @Getter
    private String message;

    @Override
    public void run() {
        try {
            log.debug("Starting job for {}", path);
            status = Status.RUNNING;
            if (singleObject) {
                createOrUpdateObject();
            }
            else {
                createOrUpdateObjects();
            }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            log.error("Error processing import job for {}", path, e);
            status = Status.FAILED;
        }
        catch (IllegalArgumentException e) {
            log.error("Invalid batch layout for batch directory {}. Leaving input in place.", path, e);
            status = Status.FAILED;
        }
        finally {
            log.debug("Job for {} finished with status {}", path, status);
        }
    }

    private void createOrUpdateObject() throws IOException, ExecutionException, InterruptedException {
        checkBatchLayout(path.getParent());
        var future = executorService.submit(new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider, acceptTimestampVersionDirectories));
        status = checkFuture(future) ? Status.SUCCESS : Status.FAILED;
    }

    private void createOrUpdateObjects() throws IOException, InterruptedException {
        checkBatchLayout(path);
        List<ObjectCreateOrUpdateTask> tasks = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path path : stream) {
                tasks.add(new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider, acceptTimestampVersionDirectories));
            }
        }
        log.info("Starting {} tasks for batch directory {}", tasks.size(), path);
        var futures = executorService.invokeAll(tasks.stream().map(Executors::callable).toList());

        if (futures.stream().allMatch(this::checkFuture)) {
            if (tasks.stream().allMatch(task -> task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS)) {
                status = Status.SUCCESS;
                log.info("All tasks for batch directory {} finished successfully", path);
                layerThresholdHandler.newTopLayerIfThresholdReached();
            }
            else {
                status = Status.FAILED;
                message = String.format("One or more tasks failed. Check error documents in '%s'.", batchOutbox);
            }
        }
        else {
            status = Status.FAILED;
            message = "One or more tasks threw an exception. Check the logs for more information.";
        }
    }

    private boolean checkFuture(Future<?> future) {
        try {
            future.get();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private void checkBatchLayout(Path path) throws IOException {
        log.debug("Validating batch layout for batch directory {}", path);
        List<Path> invalidObjectDirectories = new LinkedList<>();
        List<Path> invalidVersionDirectories = new LinkedList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path objectDir : stream) {
                var result = validateObjectImportDirectoryLayout(objectDir);
                if (!result.isObjectImportDirNameIsValid()) {
                    invalidObjectDirectories.add(objectDir);
                }
                else if (!result.getInvalidVersionDirectories().isEmpty()) {
                    invalidVersionDirectories.addAll(result.getInvalidVersionDirectories());
                }
            }
        }

        if (!invalidObjectDirectories.isEmpty() || !invalidVersionDirectories.isEmpty()) {
            throw new IllegalArgumentException("Invalid batch layout: " +
                "invalid object directories (name must match configured pattern '" + validObjectIdentifierPattern + "'): " + invalidObjectDirectories +
                ", invalid version directories (name must follow vN pattern or be a number, depending on configuration): " + invalidVersionDirectories);
        }
        log.debug("Batch layout for batch directory {} is valid", path);
    }

    private ObjectValidationResult validateObjectImportDirectoryLayout(Path objectDir) throws IOException {
        var result = new ObjectValidationResult();
        String objectDirName = objectDir.getFileName().toString();

        if (validObjectIdentifierPattern.matcher(objectDirName).matches()) {
            // Only bother to check the version directories if the object directory name is valid.
            result.setObjectImportDirNameIsValid(true);
            try (DirectoryStream<Path> versionStream = Files.newDirectoryStream(objectDir)) {
                for (Path versionDir : versionStream) {
                    if (!isValidateObjectVersionImportDirName(versionDir.getFileName().toString())) {
                        result.getInvalidVersionDirectories().add(versionDir);
                    }
                }
            }
        }
        return result;
    }

    private boolean isValidateObjectVersionImportDirName(String dirName) {
        if (acceptTimestampVersionDirectories) {
            try {
                long timestamp = Long.parseLong(dirName);
                return timestamp >= 0;
            }
            catch (NumberFormatException e) {
                return false;
            }
        }
        else {
            return dirName.startsWith("v") &&
                dirName.length() > 1 &&
                dirName.substring(1).matches("\\d+");
        }
    }
}
