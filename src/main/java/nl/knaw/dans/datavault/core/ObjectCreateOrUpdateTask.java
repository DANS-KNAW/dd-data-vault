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
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.StreamSupport;

@AllArgsConstructor
public class ObjectCreateOrUpdateTask implements Runnable {
    private final Path objectDirectory;
    private final RepositoryProvider repositoryProvider;

    @Override
    @SneakyThrows
    public void run() {
        checkVersionDirectoriesValid();
        addVersionsToRepository(getVersionDirectoriesInOrder());
    }

    private List<Path> getVersionDirectoriesInOrder() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(objectDirectory)) {
            return StreamSupport.stream(stream.spliterator(), false)
                .sorted((p1, p2) -> {
                    Long l1 = Long.parseLong(p1.getFileName().toString());
                    Long l2 = Long.parseLong(p2.getFileName().toString());
                    return l1.compareTo(l2);
                })
                .toList();
        }
    }

    private void addVersionsToRepository(List<Path> versions) throws IOException {
        for (var version : versions) {
            repositoryProvider.addVersion(objectDirectory.getFileName().toString(), version);
        }
    }

    private void checkVersionDirectoriesValid() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(objectDirectory)) {
            for (Path path : stream) {
                if (!isValidVersionDirectory(path)) {
                    throw new IllegalArgumentException("Invalid version directory: " + path);
                }
            }
        }
    }

    private boolean isValidVersionDirectory(Path dir) {
        return Files.isDirectory(dir) && isValidTimestamp(dir.getFileName().toString());
    }

    private boolean isValidTimestamp(String timestampStr) {
        try {
            long timestamp = Long.parseLong(timestampStr);
            return timestamp >= 0;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }
}
