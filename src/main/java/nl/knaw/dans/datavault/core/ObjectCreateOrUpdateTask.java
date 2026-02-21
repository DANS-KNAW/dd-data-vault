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

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.core.util.VersionDirectoryComparator;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * Task that processes an object import directory by adding its versions to the repository and moving the directory to the outbox.
 */
@Slf4j
@RequiredArgsConstructor
public class ObjectCreateOrUpdateTask implements Runnable {
    public enum Status {
        PENDING,
        RUNNING,
        SUCCESS,
        FAILED
    }

    @NonNull
    private final Path objectDirectory;
    @NonNull
    private final Path batchOutbox;
    @NonNull
    private final RepositoryProvider repositoryProvider;

    @Getter
    private Status status = Status.PENDING;

    @Override
    public void run() {
        status = Status.RUNNING;
        try {
            log.debug("Processing object directory {}", objectDirectory);
            addVersionsToRepository(getVersionDirectoriesInOrder());
            moveDirectoryToOutbox("processed", null);
            status = Status.SUCCESS;
            log.debug("Object directory {} processed successfully", objectDirectory);
        }
        catch (Exception e) {
            log.error("Error processing object directory {}", objectDirectory, e);
            try {
                status = Status.FAILED;
                moveDirectoryToOutbox("failed", e);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private List<Path> getVersionDirectoriesInOrder() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(objectDirectory)) {
            return StreamSupport.stream(stream.spliterator(), false)
                .filter(Files::isDirectory)
                .sorted(VersionDirectoryComparator.INSTANCE)
                .toList();
        }
    }

    private void addVersionsToRepository(List<Path> versions) {
        for (var version : versions) {
            var versionName = version.getFileName().toString();
            var objectId = objectDirectory.getFileName().toString();
            var versionNumber = parseVersionNumber(versionName);
            log.debug("Adding version {} to repository for object directory {}", versionName, objectDirectory);
            repositoryProvider.addVersion(objectId, versionNumber, version);
        }
    }

    private int parseVersionNumber(String name) {
        return Integer.parseInt(name.substring(1));
    }

    private void moveDirectoryToOutbox(String subdir, Exception exception) throws IOException {
        var outboxSubdir = batchOutbox.resolve(subdir);
        Files.createDirectories(outboxSubdir);
        try {
            Files.move(objectDirectory, outboxSubdir.resolve(objectDirectory.getFileName()));
            if (exception != null) {
                exception.printStackTrace(new PrintStream(outboxSubdir.resolve(objectDirectory.getFileName() + "-error.txt").toFile()));
            }
        }
        catch (IOException e) {
            log.error("Failed to move object directory {} to outbox {}", objectDirectory, outboxSubdir, e);
        }
    }
}
