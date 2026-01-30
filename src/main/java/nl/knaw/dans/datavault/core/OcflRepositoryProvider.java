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

import io.dropwizard.lifecycle.Managed;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.exception.NotFoundException;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.UnsupportedExtensionBehavior;
import io.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import io.ocfl.core.storage.OcflStorage;
import io.ocfl.core.storage.OcflStorageBuilder;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.api.OcflObjectVersionDto;
import nl.knaw.dans.layerstore.ItemsMismatchException;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.lib.ocflext.LayeredStorage;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * An implementation of {@link RepositoryProvider} that uses the OCFL library to store object versions.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE) // Builder should be used to create instances
public class OcflRepositoryProvider implements RepositoryProvider, Managed {
    @NonNull
    private final LayeredItemStore layeredItemStore;

    @NonNull
    private final Path workDir;

    @NonNull
    private final LayerConsistencyChecker layerConsistencyChecker;

    private final Path rootExtensionsSourcePath;
    private final Path rootDocsSourcePath;

    private OcflRepository ocflRepository;
    private OcflStorage ocflStorage;

    @Builder
    public static OcflRepositoryProvider create(LayeredItemStore itemStore, Path workDir, LayerConsistencyChecker layerConsistencyChecker,
        Path rootExtensionsSourcePath, Path rootDocsSourcePath) {
        return new OcflRepositoryProvider(itemStore, workDir, layerConsistencyChecker, rootExtensionsSourcePath, rootDocsSourcePath);
    }

    @Override
    public void addVersion(String objectId, int version, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as version v{}", objectVersionDirectory, objectId, version);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        // putObject wants the version number of HEAD, so we need to subtract 1 from the version number
        var versionInfoFile = objectVersionDirectory.resolveSibling(objectVersionDirectory.getFileName().toString() + ".properties");
        var reader = createVersionPropertiesReader(versionInfoFile);
        ocflRepository.putObject(ObjectVersionId.version(objectId, version - 1), objectVersionDirectory, reader.getVersionInfo());

        reader.getCustomProperties().forEach((key, value) -> updateObjectVersionProperties(objectId, version, key, value));
    }

    @Override
    public void addHeadVersion(String objectId, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as head version", objectVersionDirectory, objectId);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        var reader = createVersionPropertiesReader(objectVersionDirectory.resolveSibling(objectVersionDirectory.getFileName().toString() + ".properties"));
        ocflRepository.putObject(ObjectVersionId.head(objectId), objectVersionDirectory, reader.getVersionInfo());
        long headVersion = Optional.ofNullable(ObjectVersionId.head(objectId).getVersionNum()).map(VersionNum::getVersionNum).orElse(1L);

        reader.getCustomProperties().forEach((key, value) -> updateObjectVersionProperties(objectId, headVersion, key, value));
    }

    private void updateObjectVersionProperties(String objectId, long version, String key, Object value) {
        var ovp = new ObjectVersionProperties(layeredItemStore, ocflStorage.objectRootPath(objectId));
        try {
            ovp.load();
            ovp.putProperty(version, key, value);
            ovp.save();
            ovp.validate();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to update object version properties", e);
        }
    }

    @Override
    public Optional<OcflObjectVersionDto> getOcflObjectVersion(String objectId, int version) {
        try {
            var versionInfo = ocflRepository.getObject(ObjectVersionId.version(objectId, version));
            return Optional.of(new OcflObjectVersionDto()
                .versionNumber(version).created(versionInfo.getCreated()));
        }
        catch (NotFoundException e) {
            return Optional.empty();
        }
    }

    private VersionPropertiesReader createVersionPropertiesReader(Path versionPropertiesFile) {
        try {
            return new VersionPropertiesReader(versionPropertiesFile);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read version info from " + versionPropertiesFile, e);
        }
    }

    @Override
    public void start() {
        log.info("Starting OCFL repository provider");
        var layeredStorage = new LayeredStorage(initAndCheckTopLayer(layeredItemStore));
        ocflStorage = new OcflStorageBuilder().storage(layeredStorage).build();
        var layoutConfig = new NTupleOmitPrefixStorageLayoutConfig().setDelimiter(":").setTupleSize(3); // TODO: make configurable
        try {
            ocflRepository = new OcflRepositoryBuilder()
                .unsupportedExtensionBehavior(UnsupportedExtensionBehavior.WARN)
                .defaultLayoutConfig(layoutConfig)
                .inventoryCache(null)
                .storage(ocflStorage)
                .workDir(Files.createDirectories(workDir)).build();

            addExtensions();
            addRootDocs();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create OCFL repository", e);
        }
    }

    private LayeredItemStore initAndCheckTopLayer(LayeredItemStore layeredItemStore) {
        try {
            if (layeredItemStore.getTopLayer() == null) {
                layeredItemStore.newTopLayer();
            }
            layerConsistencyChecker.check(layeredItemStore.getTopLayer());
            return layeredItemStore;
        }
        catch (IOException | ItemsMismatchException e) {
            throw new RuntimeException("Failed to initialize top layer", e);
        }
    }

    private void addExtensions() throws IOException {
        Path tempExtensionsPath = workDir.resolve("extensions");
        if (rootExtensionsSourcePath != null) {
            if (Files.exists(rootExtensionsSourcePath)) {
                log.info("Copying storage root extensions from {} to the extensions directory", rootExtensionsSourcePath);
                copyDirectoryAndPermissions(rootExtensionsSourcePath, tempExtensionsPath);
                try (var stream = Files.list(tempExtensionsPath)) {
                    stream.forEach(path -> {
                        try {
                            if (Files.isDirectory(path)) {
                                var extensionPath = "extensions/" + path.getFileName().toString();
                                if (layeredItemStore.existsPathLike(extensionPath)) {
                                    log.info("Extension {} already exists in the OCFL repository, skipping", extensionPath);
                                }
                                else {
                                    layeredItemStore.moveDirectoryInto(path, extensionPath);
                                }
                            }
                        }
                        catch (IOException e) {
                            throw new RuntimeException("Failed to move directory " + path + " into extensions directory", e);
                        }
                    });
                }
                finally {
                    FileUtils.deleteQuietly(tempExtensionsPath.toFile());
                }
            }
            else {
                throw new IllegalStateException("Root extensions source path " + rootExtensionsSourcePath + " does not exist");
            }
        }
        else {
            log.info("Root extensions source path is not set, no extensions will be copied to the OCFL repository");
        }
    }

    private void addRootDocs() throws IOException {
        if (rootDocsSourcePath != null) {
            if (Files.exists(rootDocsSourcePath)) {
                log.info("Copying storage root docs from {} into the OCFL storage root (non-recursive)", rootDocsSourcePath);
                try (var stream = Files.list(rootDocsSourcePath)) {
                    for (Path entry : stream.toList()) {
                        if (Files.isDirectory(entry)) {
                            throw new IllegalStateException("Root docs source path must contain only files, found subdirectory: " + entry.getFileName());
                        }
                        var dest = entry.getFileName().toString();
                        try (var is = Files.newInputStream(entry)) {
                            layeredItemStore.writeFile(dest, is);
                        }
                    }
                }
            }
            else {
                throw new IllegalStateException("Root docs source path " + rootDocsSourcePath + " does not exist");
            }
        }
        else {
            log.info("Root docs source path is not set, no docs will be copied to the OCFL repository");
        }
    }

    private void copyDirectoryAndPermissions(Path source, Path target) throws IOException {
        if (Files.exists(target)) {
            FileUtils.deleteDirectory(target.toFile());
        }
        Files.walkFileTree(source, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDir = target.resolve(source.relativize(dir));
                if (!Files.exists(targetDir)) {
                    Files.createDirectory(targetDir);
                    Files.setPosixFilePermissions(targetDir, Files.getPosixFilePermissions(dir));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path targetFile = target.resolve(source.relativize(file));
                Files.copy(file, targetFile, REPLACE_EXISTING, COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
