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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

@Slf4j
@AllArgsConstructor
public class ObjectCreateOrUpdateTask implements Runnable {
    private final Path objectDirectory;
    private final Path batchOutbox;
    private final RepositoryProvider repositoryProvider;
    private final boolean acceptTimestampVersionDirectories;

    private static class VersionDirectoryComparator implements Comparator<Path> {
        @Override
        public int compare(Path p1, Path p2) {
            if (!p1.getFileName().toString().startsWith("v") || !p2.getFileName().toString().startsWith("v")) {
                throw new IllegalArgumentException("Version directory names should start with 'v'");
            }
            try {
                Long l1 = Long.parseLong(p1.getFileName().toString().substring(1));
                Long l2 = Long.parseLong(p2.getFileName().toString().substring(1));
                return l1.compareTo(l2);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Version directory names should start with 'v' followed by a number");
            }
        }
    }

    private static class TimestampDirectoryComparator implements Comparator<Path> {
        @Override
        public int compare(Path p1, Path p2) {
            try {
                Long l1 = Long.parseLong(p1.getFileName().toString());
                Long l2 = Long.parseLong(p2.getFileName().toString());
                return l1.compareTo(l2);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Timestamp directory names should be numbers");
            }
        }
    }

    @Override
    public void run() {
        try {
            addVersionsToRepository(getVersionDirectoriesInOrder());
            moveDirectoryToOutbox("processed");
        }
        catch (Exception e) {
            log.error("Error processing object directory {}", objectDirectory, e);
            moveDirectoryToOutbox("failed");
        }
    }

    private void moveDirectoryToOutbox(String subdir) {
        var outboxSubdir = batchOutbox.resolve(subdir);
        try {
            Files.move(objectDirectory, outboxSubdir.resolve(objectDirectory.getFileName()));
        }
        catch (IOException e) {
            log.error("Failed to move object directory {} to outbox {}", objectDirectory, outboxSubdir, e);
        }
    }

    private List<Path> getVersionDirectoriesInOrder() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(objectDirectory)) {
            return StreamSupport.stream(stream.spliterator(), false)
                .sorted(acceptTimestampVersionDirectories ? new TimestampDirectoryComparator() : new VersionDirectoryComparator())
                .toList();
        }
    }

    private void addVersionsToRepository(List<Path> versions) throws IOException {
        if (acceptTimestampVersionDirectories) {
            for (var version : versions) {
                repositoryProvider.addVersion(objectDirectory.getFileName().toString(), Integer.parseInt(version.getFileName().toString()), version);
            }
        }
        else {
            for (var version : versions) {
                repositoryProvider.addVersion(objectDirectory.getFileName().toString(), Integer.parseInt(version.getFileName().toString().substring(1)), version);
            }
        }
    }
}
