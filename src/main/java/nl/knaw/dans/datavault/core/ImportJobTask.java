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

import org.apache.commons.io.FileUtils;
import io.dropwizard.hibernate.UnitOfWork;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.core.ImportJob.Status;
import nl.knaw.dans.datavault.db.ImportJobDao;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

@Slf4j
@RequiredArgsConstructor
public class ImportJobTask implements Runnable {
    private final UUID id;
    /**
     * The full path to the import batch directory.
     */
    private final Path batchOrObjectImportDir;
    private final Path batchOutbox;
    private final ImportJobDao importJobDao;
    private final ExecutorService executorService;
    private final RepositoryProvider repositoryProvider;
    private final Pattern validObjectIdentifierPattern;
    private final LayerThresholdHandler layerThresholdHandler;
    private final boolean autoclean;

    private ImportJob importJob;

    @Data
    private static class ObjectValidationResult {
        private boolean objectImportDirNameIsValid;
        private final List<Path> invalidVersionDirectories = new ArrayList<>();
    }

    @UnitOfWork
    @Override
    public void run() {
        importJob = importJobDao.get(id);
        importJob.setStarted(OffsetDateTime.now());
        log.info("Starting import batch task {}", id);
        try {
            if (importJob.isSingleObject()) {
                createOrUpdateObject();
            }
            else {
                createOrUpdateObjects();
            }
        }
        catch (IllegalArgumentException e) {
            log.error("Invalid batch layout for batch directory {}. Leaving input in place.", importJob.getPath(), e);
            failed(e.getClass().getName() + ": " + e.getMessage());
        }
        catch (Exception e) {
            log.error("Error processing import batch {}", id, e);
            failed(e.getClass().getName() + ": " + e.getMessage());
        }
        finally {
            importJob.setFinished(OffsetDateTime.now());
            importJobDao.update(importJob);
        }
        log.info("Import batch task {} finished", id);
    }

    private void createOrUpdateObject() throws IOException {
        checkBatchLayout(batchOrObjectImportDir.getParent());
        var future = executorService.submit(new ObjectCreateOrUpdateTask(batchOrObjectImportDir, batchOutbox, repositoryProvider));
        if (checkFuture(future)) {
            success();
            if (autoclean) {
                // object directory was moved to outbox/processed; delete the processed entry
                deleteSilently(batchOutbox.resolve("processed").resolve(batchOrObjectImportDir.getFileName()));
                // And if the whole batch corresponds to a single object directory, clean the batch directories
                deleteBatchDirsIfSucceeded();
            }
        }
        else {
            failed("Updated failed. Check error documents in '" + batchOutbox + "'.");
        }
    }

    private void createOrUpdateObjects() throws IOException, InterruptedException {
        checkBatchLayout(batchOrObjectImportDir);
        List<ObjectCreateOrUpdateTask> tasks = new LinkedList<>();
        List<Path> objectImportDirs = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchOrObjectImportDir)) {
            for (Path path : stream) {
                tasks.add(new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider));
                objectImportDirs.add(path);
            }
        }
        log.info("Starting {} tasks for batch directory {}", tasks.size(), batchOrObjectImportDir);
        var futures = executorService.invokeAll(tasks.stream().map(Executors::callable).toList());

        if (futures.stream().allMatch(this::checkFuture)) {
            if (tasks.stream().allMatch(task -> task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS)) {
                success();
                log.info("All tasks for batch directory {} finished successfully", batchOrObjectImportDir);
                layerThresholdHandler.newTopLayerIfThresholdReached();
                if (autoclean) {
                    for (var objectImportDir : objectImportDirs) {
                        // object directories were moved to outbox/processed; delete the processed entries
                        deleteSilently(batchOutbox.resolve("processed").resolve(objectImportDir.getFileName()));
                    }
                    // remove entire batch directories from inbox and outbox
                    deleteBatchDirsIfSucceeded();
                }
            }
            else {
                // Some tasks failed. With autoclean on, delete processed entries for successful objects only.
                if (autoclean) {
                    for (int i = 0; i < tasks.size(); i++) {
                        var task = tasks.get(i);
                        if (task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS) {
                            var objectImportDir = objectImportDirs.get(i);
                            deleteSilently(batchOutbox.resolve("processed").resolve(objectImportDir.getFileName()));
                        }
                    }
                }
                failed("One or more tasks failed. Check error documents in '" + batchOutbox + "'.");
            }
        }
        else {
            failed("One or more tasks threw an exception. Check the logs for more information.");
        }
    }

    private void success() {
        importJob.setStatus(Status.SUCCESS);
    }

    private void failed(String message) {
        importJob.setStatus(Status.FAILED);
        importJob.setMessage(message);
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
            List<String> parts = getErrorParts(invalidObjectDirectories, invalidVersionDirectories);
            throw new IllegalArgumentException("Invalid batch layout: " + String.join(", ", parts));
        }
        log.debug("Batch layout for batch directory {} is valid", path);
    }

    private List<String> getErrorParts(List<Path> invalidObjectDirectories, List<Path> invalidVersionDirectories) {
        List<String> parts = new ArrayList<>();
        if (!invalidObjectDirectories.isEmpty()) {
            parts.add("invalid object directories (name must match configured pattern '" + validObjectIdentifierPattern + "'): " + invalidObjectDirectories);
        }
        if (!invalidVersionDirectories.isEmpty()) {
            parts.add("invalid version directories (name must follow vN pattern or be a number, depending on configuration): " + invalidVersionDirectories);
        }
        return parts;
    }

    private ObjectValidationResult validateObjectImportDirectoryLayout(Path objectDir) throws IOException {
        var result = new ObjectValidationResult();
        String objectDirName = objectDir.getFileName().toString();

        if (validObjectIdentifierPattern.matcher(objectDirName).matches()) {
            result.setObjectImportDirNameIsValid(true);
            List<String> versionDirNames = new ArrayList<>();
            List<String> versionInfoBaseNames = new ArrayList<>();
            List<Path> unknownEntries = new ArrayList<>();
            try (DirectoryStream<Path> versionStream = Files.newDirectoryStream(objectDir)) {
                for (Path entry : versionStream) {
                    String name = entry.getFileName().toString();
                    if (isValidObjectVersionImportDirName(name)) {
                        versionDirNames.add(name);
                    } else if (isValidVersionPropertiesFileName(name)) {
                        // Strip .json extension
                        versionInfoBaseNames.add(name.substring(0, name.length() - ".json".length()));
                    } else {
                        unknownEntries.add(entry);
                    }
                }
            }
            // Sets must match exactly
            var versionDirSet = new HashSet<>(versionDirNames);
            var versionInfoSet = new HashSet<>(versionInfoBaseNames);
            if (!versionDirSet.equals(versionInfoSet)) {
                // Add all mismatched version directories and info files to invalidVersionDirectories
                for (String dir : versionDirSet) {
                    if (!versionInfoSet.contains(dir)) {
                        result.getInvalidVersionDirectories().add(objectDir.resolve(dir));
                    }
                }
                for (String info : versionInfoSet) {
                    if (!versionDirSet.contains(info)) {
                        result.getInvalidVersionDirectories().add(objectDir.resolve(info + ".json"));
                    }
                }
            }
            // Add unknown entries
            result.getInvalidVersionDirectories().addAll(unknownEntries);
        }
        return result;
    }

    private boolean isValidObjectVersionImportDirName(String dirName) {
        return dirName.startsWith("v") &&
            dirName.length() > 1 &&
            dirName.substring(1).matches("\\d+");
    }

    private boolean isValidVersionPropertiesFileName(String fileName) {
        return fileName.matches("v\\d+\\.json");
    }

    private void deleteBatchDirsIfSucceeded() {
        try {
            deleteSilently(batchOrObjectImportDir);
            deleteSilently(batchOutbox);
        }
        catch (Exception e) {
            log.warn("Autoclean failed to delete batch directories for {}", importJob.getPath(), e);
        }
    }

    private void deleteSilently(Path path) {
        try {
            if (Files.exists(path)) {
                var file = path.toFile();
                if (file.isDirectory()) {
                    FileUtils.deleteDirectory(file);
                }
                else {
                    Files.deleteIfExists(path);
                }
            }
        }
        catch (IOException e) {
            log.debug("Failed to delete {}", path, e);
        }
    }
}
