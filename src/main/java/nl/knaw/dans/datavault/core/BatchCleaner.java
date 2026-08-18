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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Helper class for cleaning up processed objects and batch directories of an import batch.
 */
@Slf4j
public class BatchCleaner {
    private final Path batchOrObjectImportDir;
    private final Path batchOutbox;

    public BatchCleaner(Path batchOutbox) {
        this(null, batchOutbox);
    }

    public BatchCleaner(Path batchOrObjectImportDir, Path batchOutbox) {
        this.batchOrObjectImportDir = batchOrObjectImportDir;
        this.batchOutbox = batchOutbox;
    }

    public void cleanProcessedObject(Path objectImportDir) {
        cleanProcessedObject(objectImportDir.getFileName().toString());
    }

    public void cleanProcessedObject(String objectName) {
        if (batchOutbox != null) {
            deleteSilently(batchOutbox.resolve("processed").resolve(objectName));
        }
    }

    public void cleanProcessedObjects(List<Path> objectImportDirs) {
        for (var objectImportDir : objectImportDirs) {
            cleanProcessedObject(objectImportDir);
        }
    }

    public void cleanProcessedObjects() {
        if (batchOutbox != null) {
            var processedDir = batchOutbox.resolve("processed");
            if (Files.exists(processedDir) && Files.isDirectory(processedDir)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(processedDir)) {
                    for (var path : stream) {
                        deleteSilently(path);
                    }
                }
                catch (IOException e) {
                    log.debug("Failed to list processed directory {}", processedDir, e);
                }
            }
        }
    }

    public void cleanSuccessfulProcessedObjects(List<ObjectCreateOrUpdateTask> tasks, List<Path> objectImportDirs) {
        for (int i = 0; i < tasks.size(); i++) {
            var task = tasks.get(i);
            if (task.getStatus() == ObjectCreateOrUpdateTask.Status.SUCCESS) {
                cleanProcessedObject(objectImportDirs.get(i));
            }
        }
    }

    public void deleteBatchDirsIfSucceeded() {
        deleteBatchDirs();
    }

    public void deleteBatchDirs() {
        try {
            if (batchOrObjectImportDir != null) {
                deleteSilently(batchOrObjectImportDir);
            }
            if (batchOutbox != null) {
                deleteSilently(batchOutbox);
            }
        }
        catch (Exception e) {
            log.warn("Autoclean failed to delete batch directories for {}", batchOrObjectImportDir, e);
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
