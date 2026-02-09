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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.JsonPath;
import nl.knaw.dans.datavault.config.RootExtensionsInitEdit;
import io.dropwizard.lifecycle.Managed;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.exception.NotFoundException;
import io.ocfl.api.model.ObjectVersionId;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * An implementation of {@link RepositoryProvider} that uses the OCFL library to store object versions.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE) // Builder should be used to create instances
public class OcflRepositoryProvider implements RepositoryProvider, Managed {
    private static final Set<PosixFilePermission> DIR_PERMISSIONS = Set.of(
        PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
        PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE);
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE);

    @NonNull
    private final LayeredItemStore layeredItemStore;

    @NonNull
    private final Path workDir;

    @NonNull
    private final LayerConsistencyChecker layerConsistencyChecker;

    private final Path rootExtensionsSourcePath;
    private final Path rootDocsSourcePath;
    private final List<RootExtensionsInitEdit> rootExtensionsInitEdits;

    private OcflRepository ocflRepository;
    private OcflStorage ocflStorage;
    private PropertyRegistryValidator propertyRegistryValidator;
    private ObjectVersionPropertiesValidator objectVersionPropertiesValidator;

    @Builder
    public static OcflRepositoryProvider create(LayeredItemStore itemStore, Path workDir, LayerConsistencyChecker layerConsistencyChecker,
        Path rootExtensionsSourcePath, Path rootDocsSourcePath, List<RootExtensionsInitEdit> rootExtensionsInitEdits) {
        return new OcflRepositoryProvider(itemStore, workDir, layerConsistencyChecker, rootExtensionsSourcePath, rootDocsSourcePath, rootExtensionsInitEdits);
    }

    @Override
    public void addVersion(String objectId, int version, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as version v{}", objectVersionDirectory, objectId, version);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        var versionInfoFile = objectVersionDirectory.resolveSibling(objectVersionDirectory.getFileName().toString() + ".json");
        var reader = createVersionPropertiesReader(versionInfoFile);
        // Validate custom properties against the storage-root property registry before writing anything
        propertyRegistryValidator.validate(reader.getCustomProperties());

        // Precompute candidate object_version_properties for the new version and validate against schema
        var ovp = new ObjectVersionProperties(layeredItemStore, ocflStorage.objectRootPath(objectId));
        try {
            ovp.load();
            reader.getCustomProperties().forEach((key, value) -> ovp.putProperty(version, key, value));
            objectVersionPropertiesValidator.validate(ovp.getProperties());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to prepare object version properties", e);
        }

        // putObject wants the version number of HEAD, so we need to subtract 1 from the version number
        ocflRepository.putObject(ObjectVersionId.version(objectId, version - 1), objectVersionDirectory, reader.getVersionInfo());
        ovp.save();
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
            propertyRegistryValidator = new PropertyRegistryValidator(layeredItemStore);
            objectVersionPropertiesValidator = new ObjectVersionPropertiesValidator(layeredItemStore);
            log.info("Validating OCFL repository property registry");
            propertyRegistryValidator.validate();
            log.info("OCFL repository property registry OK");
            PackagingFormatRegistryValidator packagingFormatRegistryValidator = new PackagingFormatRegistryValidator(layeredItemStore);
            log.info("Validating OCFL repository packaging format registry");
            packagingFormatRegistryValidator.validate();
            log.info("OCFL repository packaging format registry OK");
            log.info("OCFL repository provider started");
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
                copyDirectoryAndSetPermissions(rootExtensionsSourcePath, tempExtensionsPath);
                // Apply initialization edits if configured
                applyRootExtensionsInitEdits(tempExtensionsPath);
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
                        var destFileName = entry.getFileName().toString();
                        try (var is = Files.newInputStream(entry)) {
                            layeredItemStore.writeFile(destFileName, is);
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

    private void copyDirectoryAndSetPermissions(Path source, Path target) throws IOException {
        if (Files.exists(target)) {
            FileUtils.deleteDirectory(target.toFile());
        }
        Files.walkFileTree(source, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDir = target.resolve(source.relativize(dir));
                if (!Files.exists(targetDir)) {
                    Files.createDirectory(targetDir);
                    Files.setPosixFilePermissions(targetDir, DIR_PERMISSIONS);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path targetFile = target.resolve(source.relativize(file));
                Files.copy(file, targetFile, REPLACE_EXISTING, COPY_ATTRIBUTES);
                // Set permissions to rw for owner and group, and none for others
                Files.setPosixFilePermissions(targetFile, FILE_PERMISSIONS);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void applyRootExtensionsInitEdits(Path tempExtensionsPath) throws IOException {
        var edits = getRootExtensionsInitEdits();
        if (edits.isEmpty()) {
            return;
        }
        var jsonPathConf = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL).build();
        for (var edit : edits) {
            var targetFile = tempExtensionsPath.resolve(edit.getFile());
            if (!Files.exists(targetFile)) {
                throw new IllegalStateException("Root extension init edit target does not exist: " + edit.getFile());
            }
            var content = Files.readString(targetFile);
            var document = JsonPath.parse(content, jsonPathConf);
            var matchesRaw = document.read(edit.getJsonPath());
            var current = getNodeToReplace(edit, matchesRaw);
            var replacement = edit.getValue();
            if (!isTypeCompatible(current, replacement)) {
                throw new IllegalStateException("Type mismatch for JSONPath " + edit.getJsonPath() + " in file " + edit.getFile() +
                    ": existing type is " + current.getClass().getSimpleName() +
                    ", replacement type is " + replacement.getClass().getSimpleName());
            }
            document.set(edit.getJsonPath(), replacement);
            var updated = document.jsonString();
            Files.writeString(targetFile, updated);
            log.info("Applied root extension init edit on {} at {}", edit.getFile(), edit.getJsonPath());
        }
    }

    private static Object getNodeToReplace(RootExtensionsInitEdit edit, Object matchesRaw) {
        List<?> matches = matchesRaw instanceof List ? (List<?>) matchesRaw : List.of(matchesRaw);
        if (matches.isEmpty()) {
            throw new IllegalStateException("JSONPath did not match any nodes: " + edit.getJsonPath() + " in file " + edit.getFile());
        }
        if (matches.size() != 1) {
            throw new IllegalStateException("JSONPath matched multiple nodes (" + matches.size() + ") for " + edit.getJsonPath() + " in file " + edit.getFile());
        }
        var current = matches.get(0);
        return current;
    }

    private boolean isTypeCompatible(Object current, Object replacement) {
        if (current == null || replacement == null) {
            // Allow setting null or replacing a null with any type
            return true;
        }
        var currentClass = current.getClass();
        var replClass = replacement.getClass();
        // Allow numeric replacements across numeric types
        if (Number.class.isAssignableFrom(currentClass) && Number.class.isAssignableFrom(replClass)) {
            return true;
        }
        // Allow boolean to boolean
        if (current instanceof Boolean && replacement instanceof Boolean) {
            return true;
        }
        // Allow string to string
        if (current instanceof CharSequence && replacement instanceof CharSequence) {
            return true;
        }
        // Allow map to map
        if (current instanceof java.util.Map && replacement instanceof java.util.Map) {
            return true;
        }
        // Allow list to list
        if (current instanceof java.util.List && replacement instanceof java.util.List) {
            return true;
        }
        // Otherwise, types are incompatible
        return false;
    }

    private List<RootExtensionsInitEdit> getRootExtensionsInitEdits() {
        return rootExtensionsInitEdits != null ? rootExtensionsInitEdits : List.of();
    }
}
