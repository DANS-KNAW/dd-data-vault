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
package nl.knaw.dans.datavault.resources;

import lombok.NonNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The local directory that the item store copy endpoints may read from and write to. Paths given by clients are checked to lie inside this directory,
 * also after resolving symbolic links, so that these endpoints cannot be used to read or create arbitrary files on the server.
 * <p>
 * The error messages deliberately do not contain the paths, so that they do not reveal anything about the server's file system.
 */
class ExchangeDir {
    private final Path configured;

    ExchangeDir(@NonNull Path configured) {
        this.configured = configured.toAbsolutePath().normalize();
    }

    /**
     * Checks a path to an existing regular file to read from.
     *
     * @param path the path given by the client
     * @param role the role of the path in the request, used in error messages (e.g. "Source")
     * @return the real path of the file
     * @throws IllegalArgumentException if the path is not acceptable
     * @throws IOException              if the exchange directory or the path cannot be resolved
     */
    Path existingFile(String path, String role) throws IOException {
        var real = existing(path, role);
        if (!Files.isRegularFile(real)) {
            throw new IllegalArgumentException(role + " path is not a regular file");
        }
        return real;
    }

    /**
     * Checks a path to an existing directory to read from. The directory must not contain symbolic links, because copying it would follow them.
     *
     * @param path the path given by the client
     * @param role the role of the path in the request, used in error messages (e.g. "Source")
     * @return the real path of the directory
     * @throws IllegalArgumentException if the path is not acceptable
     * @throws IOException              if the exchange directory or the path cannot be resolved
     */
    Path existingDirectoryWithoutSymlinks(String path, String role) throws IOException {
        var real = existing(path, role);
        if (!Files.isDirectory(real)) {
            throw new IllegalArgumentException(role + " path is not a directory");
        }
        try (var entries = Files.walk(real)) {
            if (entries.anyMatch(Files::isSymbolicLink)) {
                throw new IllegalArgumentException(role + " directory contains symbolic links");
            }
        }
        return real;
    }

    /**
     * Checks a path to a file that is to be created. The parent directory must exist and the file itself must not.
     *
     * @param path the path given by the client
     * @param role the role of the path in the request, used in error messages (e.g. "Destination")
     * @return the path to create, with the parent directory as a real path
     * @throws IllegalArgumentException if the path is not acceptable
     * @throws IOException              if the exchange directory or the path cannot be resolved
     */
    Path newFile(String path, String role) throws IOException {
        var normalized = absoluteInside(path, role);
        var parent = normalized.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new IllegalArgumentException(role + " parent directory does not exist");
        }
        var target = requireInside(parent.toRealPath(), role).resolve(normalized.getFileName());
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException(role + " already exists");
        }
        return target;
    }

    private Path existing(String path, String role) throws IOException {
        var normalized = absoluteInside(path, role);
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException(role + " path does not exist");
        }
        return requireInside(normalized.toRealPath(), role);
    }

    /**
     * Checks that the path is absolute and, after normalization, lexically inside the exchange directory. This is checked before anything is looked up in
     * the file system, so that a client cannot find out whether paths outside the exchange directory exist.
     */
    private Path absoluteInside(String path, String role) throws IOException {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException(role + " path is required");
        }
        var p = Paths.get(path);
        if (!p.isAbsolute()) {
            throw new IllegalArgumentException(role + " path must be absolute");
        }
        var normalized = p.normalize();
        // The configured path may itself be a symbolic link; accept both the configured and the real form
        if (!normalized.startsWith(configured) && !normalized.startsWith(realRoot())) {
            throw outside(role);
        }
        return normalized;
    }

    private Path requireInside(Path realPath, String role) throws IOException {
        if (!realPath.startsWith(realRoot())) {
            throw outside(role);
        }
        return realPath;
    }

    private Path realRoot() throws IOException {
        return configured.toRealPath();
    }

    private IllegalArgumentException outside(String role) {
        return new IllegalArgumentException(role + " path must be inside the exchange directory (dataVault.itemstore.exchangeDir)");
    }
}
