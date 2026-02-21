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
        private boolean hasNonConsecutiveVersions;
        private final List<Path> invalidVersionDirectories = new ArrayList<>();
    }

    @UnitOfWork
    @Override
    public void run() {
        importJob = importJobDao.get(id);
        importJob.setStarted(OffsetDateTime.now());
        log.info("Starting import batch task {}", id);
        try {
            processImportJob();
        }
        catch (IllegalArgumentException e) {
            handleBatchLayoutError(e);
        }
        catch (Exception e) {
            handleProcessingError(e);
        }
        finally {
            finishImportJob();
        }
        log.info("Import batch task {} finished", id);
    }

    private void processImportJob() throws IOException, InterruptedException {
        if (importJob.isSingleObject()) {
            processSingleObjectImport();
        } else {
            processBatchObjectImport();
        }
    }

    private void handleBatchLayoutError(IllegalArgumentException e) {
        log.error("Invalid batch layout for batch directory {}. Leaving input in place.", importJob.getPath(), e);
        failed(e.getClass().getName() + ": " + e.getMessage());
    }

    private void handleProcessingError(Exception e) {
        log.error("Error processing import batch {}", id, e);
        failed(e.getClass().getName() + ": " + e.getMessage());
    }

    private void finishImportJob() {
        importJob.setFinished(OffsetDateTime.now());
        importJobDao.update(importJob);
    }

    private void processSingleObjectImport() throws IOException {
        checkBatchLayout(batchOrObjectImportDir.getParent());
        var future = executorService.submit(new ObjectCreateOrUpdateTask(batchOrObjectImportDir, batchOutbox, repositoryProvider));
        handleObjectImportResult(future);
    }

    private void handleObjectImportResult(Future<?> future) {
        if (checkFuture(future)) {
            success();
            if (autoclean) {
                cleanProcessedObject(batchOrObjectImportDir);
                deleteBatchDirsIfSucceeded();
            }
        } else {
            failed("Updated failed. Check error documents in '" + batchOutbox + "'.");
        }
    }

    private void processBatchObjectImport() throws IOException, InterruptedException {
        checkBatchLayout(batchOrObjectImportDir);
        var tasks = createObjectTasks(batchOrObjectImportDir);
        var objectImportDirs = getObjectImportDirs(batchOrObjectImportDir);
        log.info("Starting {} tasks for batch directory {}", tasks.size(), batchOrObjectImportDir);
        @SuppressWarnings("unchecked")
        var futures = (List<Future<?>>)(List<?>)executorService.invokeAll(tasks.stream().map(Executors::callable).toList());
        handleBatchImportResults(tasks, objectImportDirs, futures);
    }

    private List<ObjectCreateOrUpdateTask> createObjectTasks(Path batchDir) throws IOException {
        var tasks = new LinkedList<ObjectCreateOrUpdateTask>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchDir)) {
            for (Path path : stream) {
                tasks.add(new ObjectCreateOrUpdateTask(path, batchOutbox, repositoryProvider));
            }
        }
        return tasks;
    }

    private List<Path> getObjectImportDirs(Path batchDir) throws IOException {
        var dirs = new LinkedList<Path>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchDir)) {
            for (Path path : stream) {
                dirs.add(path);
            }
        }
        return dirs;
    }

    private void handleBatchImportResults(List<ObjectCreateOrUpdateTask> tasks, List<Path> objectImportDirs, List<Future<?>> futures) {
        if (futures.stream().allMatch(this::checkFuture)) {
            if (tasks.stream().allMatch(task -> task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS)) {
                success();
                log.info("All tasks for batch directory {} finished successfully", batchOrObjectImportDir);
                try {
                    layerThresholdHandler.newTopLayerIfThresholdReached();
                } catch (IOException e) {
                    log.error("Error updating top layer after threshold reached", e);
                }
                if (autoclean) {
                    cleanProcessedObjects(objectImportDirs);
                    deleteBatchDirsIfSucceeded();
                }
            } else {
                if (autoclean) {
                    cleanSuccessfulProcessedObjects(tasks, objectImportDirs);
                }
                failed("One or more tasks failed. Check error documents in '" + batchOutbox + "'.");
            }
        } else {
            failed("One or more tasks threw an exception. Check the logs for more information.");
        }
    }

    private void cleanProcessedObject(Path objectImportDir) {
        deleteSilently(batchOutbox.resolve("processed").resolve(objectImportDir.getFileName()));
    }

    private void cleanProcessedObjects(List<Path> objectImportDirs) {
        for (var objectImportDir : objectImportDirs) {
            cleanProcessedObject(objectImportDir);
        }
    }

    private void cleanSuccessfulProcessedObjects(List<ObjectCreateOrUpdateTask> tasks, List<Path> objectImportDirs) {
        for (int i = 0; i < tasks.size(); i++) {
            var task = tasks.get(i);
            if (task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS) {
                cleanProcessedObject(objectImportDirs.get(i));
            }
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
        List<Path> invalidObjectImportDirectories = new LinkedList<>();
        List<Path> invalidVersionDirectories = new LinkedList<>();
        List<Path> nonConsecutiveVersionDirs = new LinkedList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            for (Path objectDir : stream) {
                var result = validateObjectImportDirectoryLayout(objectDir);
                if (!result.isObjectImportDirNameIsValid()) {
                    invalidObjectImportDirectories.add(objectDir);
                }
                else {
                    if (!result.getInvalidVersionDirectories().isEmpty()) {
                        invalidVersionDirectories.addAll(result.getInvalidVersionDirectories());
                    }
                    if (result.isHasNonConsecutiveVersions()) {
                        nonConsecutiveVersionDirs.add(objectDir);
                    }
                }
            }
        }

        if (!invalidObjectImportDirectories.isEmpty() || !invalidVersionDirectories.isEmpty() || !nonConsecutiveVersionDirs.isEmpty()) {
            List<String> parts = getErrorParts(invalidObjectImportDirectories, invalidVersionDirectories, nonConsecutiveVersionDirs);
            throw new IllegalArgumentException("Invalid batch layout: " + String.join(", ", parts));
        }
        log.debug("Batch layout for batch directory {} is valid", path);
    }

    private List<String> getErrorParts(List<Path> invalidObjectDirectories, List<Path> invalidVersionDirectories, List<Path> nonConsecutiveVersionDirs) {
        List<String> parts = new ArrayList<>();
        if (!invalidObjectDirectories.isEmpty()) {
            parts.add("invalid object directories (name must match configured pattern '" + validObjectIdentifierPattern + "'): " + invalidObjectDirectories);
        }
        if (!invalidVersionDirectories.isEmpty()) {
            parts.add("invalid version directories (name must follow vN pattern or be a number, depending on configuration): " + invalidVersionDirectories);
        }
        if (!nonConsecutiveVersionDirs.isEmpty()) {
            parts.add("non-consecutive version directories: " + nonConsecutiveVersionDirs);
        }
        return parts;
    }

    private ObjectValidationResult validateObjectImportDirectoryLayout(Path objectImportDir) throws IOException {
        var result = new ObjectValidationResult(); // Invalid to start with
        String objectImportDirName = objectImportDir.getFileName().toString();

        if (validObjectIdentifierPattern.matcher(objectImportDirName).matches()) {
            result.setObjectImportDirNameIsValid(true);
            var entryClassification = classifyObjectDirEntries(objectImportDir);
            var versionDirNames = entryClassification.versionDirNames;
            var versionInfoBaseNames = entryClassification.versionInfoBaseNames;
            var unknownEntries = entryClassification.unknownEntries;

            addSetMismatchInvalidEntries(objectImportDir, versionDirNames, versionInfoBaseNames, result);
            result.getInvalidVersionDirectories().addAll(unknownEntries);
            addNonConsecutiveVersionDirs(objectImportDir, versionDirNames, result);
        }
        return result;
    }

    private static class EntryClassification {
        List<String> versionDirNames = new ArrayList<>();
        List<String> versionInfoBaseNames = new ArrayList<>();
        List<Path> unknownEntries = new ArrayList<>();
    }

    private EntryClassification classifyObjectDirEntries(Path objectDir) throws IOException {
        var classification = new EntryClassification();
        try (DirectoryStream<Path> versionStream = Files.newDirectoryStream(objectDir)) {
            for (var entry : versionStream) {
                var name = entry.getFileName().toString();
                if (isValidObjectVersionImportDirName(name)) {
                    classification.versionDirNames.add(name);
                } else if (isValidVersionPropertiesFileName(name)) {
                    classification.versionInfoBaseNames.add(name.substring(0, name.length() - ".json".length()));
                } else {
                    classification.unknownEntries.add(entry);
                }
            }
        }
        return classification;
    }

    private void addSetMismatchInvalidEntries(Path objectDir, List<String> versionDirNames, List<String> versionInfoBaseNames, ObjectValidationResult result) {
        var versionDirSet = new HashSet<>(versionDirNames);
        var versionInfoSet = new HashSet<>(versionInfoBaseNames);
        if (!versionDirSet.equals(versionInfoSet)) {
            for (var dir : versionDirSet) {
                if (!versionInfoSet.contains(dir)) {
                    result.getInvalidVersionDirectories().add(objectDir.resolve(dir));
                }
            }
            for (var info : versionInfoSet) {
                if (!versionDirSet.contains(info)) {
                    result.getInvalidVersionDirectories().add(objectDir.resolve(info + ".json"));
                }
            }
        }
    }

    private void addNonConsecutiveVersionDirs(Path objectDir, List<String> versionDirNames, ObjectValidationResult result) {
        if (!versionDirNames.isEmpty()) {
            var versionNumbers = new ArrayList<Integer>();
            for (var dirName : versionDirNames) {
                try {
                    versionNumbers.add(Integer.parseInt(dirName.substring(1))); // skip 'v'
                } catch (NumberFormatException e) {
                    result.getInvalidVersionDirectories().add(objectDir.resolve(dirName));
                }
            }
            versionNumbers.sort(Integer::compareTo);
            boolean foundNonConsecutive = false;
            for (int i = 1; i < versionNumbers.size(); i++) {
                if (versionNumbers.get(i) != versionNumbers.get(i - 1) + 1) {
                    foundNonConsecutive = true;
                }
            }
            result.setHasNonConsecutiveVersions(foundNonConsecutive);
        }
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
