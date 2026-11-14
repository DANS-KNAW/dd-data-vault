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
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Runs dmftar on the local file system only; no network actions are involved.
 */
@Slf4j
public class LocalDmfTarRunner {
    private static final String CACHE_DIR_PREFIX = "dmftar-cache.";

    private final Path dmfTarExecutable;

    public LocalDmfTarRunner(Path dmfTarExecutable) {
        this.dmfTarExecutable = dmfTarExecutable;
    }

    /**
     * Creates a dmftar directory at the target path from the contents of the directory (not including the directory itself). dmftar creates temporary cache directories in the working
     * directory; these are removed, also if the command fails.
     */
    public void tarDirectory(Path directory, Path target) throws IOException {
        removeCacheDirectories(directory);
        var commandLine = new CommandLine(dmfTarExecutable.toAbsolutePath().toString())
            .addArgument("-cf")
            .addArgument(target.toAbsolutePath().toString(), false)
            .addArgument(".");
        var executor = DefaultExecutor.builder().setWorkingDirectory(directory.toAbsolutePath().toFile()).get();
        try {
            executor.execute(commandLine);
        }
        finally {
            removeCacheDirectories(directory);
        }
    }

    private void removeCacheDirectories(Path directory) throws IOException {
        var cacheDirs = directory.toFile().listFiles(file -> file.isDirectory() && file.getName().startsWith(CACHE_DIR_PREFIX));
        if (cacheDirs != null) {
            for (var cacheDir : Arrays.asList(cacheDirs)) {
                log.warn("Removing dmftar cache directory {}", cacheDir);
                FileUtils.deleteDirectory(cacheDir);
            }
        }
    }
}
