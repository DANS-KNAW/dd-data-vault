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
import lombok.Getter;
import lombok.SneakyThrows;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@Builder(builderClassName = "Builder")
public class JobTask implements Runnable {
    private final Path batchDirectory;
    private final Pattern validObjectIdentifierPattern;
    private final ExecutorService executorService;
    private final RepositoryProvider repositoryProvider;

    @Getter
    private boolean finished;

    public static class Builder {
        public JobTask.Builder validObjectIdentifierPattern(String validObjectIdentifierPattern) {
            this.validObjectIdentifierPattern = Pattern.compile(validObjectIdentifierPattern);
            return this;
        }
    }

    @Override
    @SneakyThrows
    public void run() {
        validateBatchLayout();
        List<Callable<Object>> tasks = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchDirectory)) {
            for (Path path : stream) {
                tasks.add(Executors.callable(new ObjectCreateOrUpdateTask(path, repositoryProvider)));
            }
        }
        executorService.invokeAll(tasks);
        finished = true;
    }

    /*
     * A valid batch layout has the following structure. The batch directory contains a number of directories, each representing an object. The name of
     * each object directory is the object identifier. The valid pattern for the object identifier is given by the validObjectIdentifierPattern.
     * Each object directory contains a number of directories, each representing a version of the object. The name of each version directory is a timestamp,
     * which establishes the order of the versions. The content of each version directory is the content of the version and may consist of an entire
     * directory tree.
     *
     * Example:
     *
     *  ├── urn:nbn:nl:ui:13-26febff0-4fd4-4ee7-8a96-b0703b96f812
     *  │   ├── 1706877020546
     *  │   │   └── content
     *  │   ├── 1706877024159
     *  │   │   └── content
     *  │   └── 1706877025840
     *  │       └── content
     *  ├── urn:nbn:nl:ui:13-2ced2354-3a9d-44b1-a594-107b3af99789
     *  │   └── 1706877038911
     *  │       └── content
     *  └── urn:nbn:nl:ui:13-b7c0742f-a9b2-4c11-bffe-615dbe24c8a0
     *       └── 1706877046791
     *           └── content
     *
     * In this example an object identifier must be a DANS URN:NBN value.
     */
    @SneakyThrows
    private void validateBatchLayout() {
        List<Path> invalidObjectDirectories = new LinkedList<>();
        List<Path> invalidVersionDirectories = new LinkedList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchDirectory)) {
            for (Path objectDir : stream) {
                String objectDirName = objectDir.getFileName().toString();
                if (!validObjectIdentifierPattern.matcher(objectDirName).matches()) {
                    invalidObjectDirectories.add(objectDir);
                }

                try (DirectoryStream<Path> versionStream = Files.newDirectoryStream(objectDir)) {
                    for (Path versionDir : versionStream) {
                        if (!isValidTimestamp(versionDir.getFileName().toString())) {
                            invalidVersionDirectories.add(versionDir);
                        }
                    }
                }
            }
        }

        if (!invalidObjectDirectories.isEmpty() || !invalidVersionDirectories.isEmpty()) {
            throw new IllegalArgumentException("Invalid batch layout: " +
                "invalid object directories: " + invalidObjectDirectories +
                ", invalid version directories: " + invalidVersionDirectories);
        }
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
