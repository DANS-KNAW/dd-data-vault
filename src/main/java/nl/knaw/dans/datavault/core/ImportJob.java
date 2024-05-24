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

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

@Builder(builderClassName = "Builder")
@Slf4j
public class ImportJob implements Runnable {
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

    private final Pattern validObjectIdentifierPattern;
    private final ExecutorService executorService;
    private final RepositoryProvider repositoryProvider;
    private final boolean acceptTimestampVersionDirectories;

    @Default
    @Getter
    private final UUID id = UUID.randomUUID();

    @Getter
    private final Path path;

    private final Path batchOutbox;

    @Getter
    private final boolean singleObject;

    @Default
    @Getter
    private Status status = Status.PENDING;

    @Override
    @SneakyThrows
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
        catch (Exception e) {
            log.error("Job for {} failed", path, e);
            status = Status.FAILED;
            throw e;
        }
    }

    private void createOrUpdateObject() {
        try {
            checkBatchLayout(path.getParent());
            new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider, acceptTimestampVersionDirectories).run();
            status = Status.SUCCESS;
        }
        catch (Exception e) {
            status = Status.FAILED;
            throw e;
        }
    }

    @SneakyThrows
    private void createOrUpdateObjects() {
        checkBatchLayout(path);
        List<Callable<Object>> tasks = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path path : stream) {
                tasks.add(Executors.callable(new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider, acceptTimestampVersionDirectories)));
            }
        }
        log.info("Starting {} tasks for batch directory {}", tasks.size(), path);
        var futures = executorService.invokeAll(tasks);
        status = futures.stream().allMatch(this::checkFuture) ? Status.SUCCESS : Status.FAILED;
    }

    private boolean checkFuture(Future<Object> future) {
        try {
            future.get();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    
    @SneakyThrows
    private void checkBatchLayout(Path path) {
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
                "invalid object directories: " + invalidObjectDirectories +
                ", invalid version directories: " + invalidVersionDirectories);
        }
        log.debug("Batch layout for batch directory {} is valid", path);
    }

    @SneakyThrows
    private ObjectValidationResult validateObjectImportDirectoryLayout(Path objectDir) {
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
