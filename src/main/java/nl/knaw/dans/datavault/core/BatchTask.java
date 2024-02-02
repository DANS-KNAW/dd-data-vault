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
public class BatchTask implements Runnable {
    private final Path batchDirectory;
    private final Pattern validObjectIdentifierPattern;
    private final ExecutorService executorService;
    private final RepositoryProvider repositoryProvider;

    public static class Builder {
        public BatchTask.Builder validObjectIdentifierPattern(String validObjectIdentifierPattern) {
            this.validObjectIdentifierPattern = Pattern.compile(validObjectIdentifierPattern);
            return this;
        }
    }

    @Override
    @SneakyThrows
    public void run() {
        List<Callable<Object>> tasks = new LinkedList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(batchDirectory)) {
            for (Path path : stream) {
                String directoryName = path.getFileName().toString();
                if (validObjectIdentifierPattern.matcher(directoryName).matches()) {
                    tasks.add(Executors.callable(new ObjectCreateOrUpdateTask(path, repositoryProvider)));
                }
            }
        }
        executorService.invokeAll(tasks);
    }
}
