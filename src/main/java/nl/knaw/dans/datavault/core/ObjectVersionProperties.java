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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.ocfl.core.storage.OcflStorage;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.layerstore.ItemStore;
import org.apache.commons.codec.binary.Hex;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
@RequiredArgsConstructor
public class ObjectVersionProperties {
    private static final String EXTENSION_DIR = "object-version-properties";
    private static final String VERSION_PROPERTIES_FILE = "object_version_properties.json";
    private static final String SIDE_CAR_FILE = "object_version_properties.json.sha512"; // TODO: algorithm should be configurable
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ItemStore itemStore;
    private final String objectRootPath;

    private Map<String, Map<String, Object>> properties = new HashMap<>();

    public void load() throws IOException {
        var propertiesJsonFile = getExtensionDir().resolve(VERSION_PROPERTIES_FILE);
        itemStore.createDirectory(propertiesJsonFile.getParent().toString());
        if (!itemStore.existsPathLike(propertiesJsonFile.toString())) {
            properties = new HashMap<>();
        }
        else {
            try (var is = itemStore.readFile(propertiesJsonFile.toString())) {
                properties = mapper.readValue(is,
                    mapper.getTypeFactory().constructMapType(Map.class, String.class, Map.class));
            }
        }
    }

    private Path getExtensionDir() {
        return Path.of(objectRootPath).resolve("extensions").resolve(EXTENSION_DIR);
    }

    public void validate() throws IOException {
        if (properties == null) {
            throw new IllegalStateException("Properties have not been loaded");
        }

        var versionKeys = properties.keySet();
        var versionSubDirs = itemStore.listDirectory(objectRootPath).stream()
            .map(p -> Path.of(p.getPath()).getFileName().toString())
            .filter(n -> n.startsWith("v")).collect(Collectors.toSet());

        var keysWithoutVersionDirs = Sets.difference(versionKeys, versionSubDirs);
        var versionDirsWithoutKeys = Sets.difference(versionSubDirs, versionKeys);

        if (!keysWithoutVersionDirs.isEmpty()) {
            throw new IllegalStateException("Properties contain keys that do not match version directories: " + keysWithoutVersionDirs);
        }
        if (!versionDirsWithoutKeys.isEmpty()) {
            throw new IllegalStateException("Version directories that do not have corresponding properties: " + versionDirsWithoutKeys);
        }

        // Check that sidecar file exists
        var sidecarFile = getExtensionDir().resolve(SIDE_CAR_FILE);
        if (!itemStore.existsPathLike(sidecarFile.toString())) {
            throw new IllegalStateException("Sidecar file does not exist: " + sidecarFile);
        }

        // TODO: Validate the content of the sidecar file.
    }

    public void save() {
        if (properties == null) {
            throw new IllegalStateException("Properties have not been loaded");
        }
        var propertiesJsonFile = getExtensionDir().resolve(VERSION_PROPERTIES_FILE);

        try {
            itemStore.createDirectory(propertiesJsonFile.getParent().toString());
            itemStore.writeFile(propertiesJsonFile.toString(), new ByteArrayInputStream(mapper.writeValueAsString(properties).getBytes(StandardCharsets.UTF_8)));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write object version properties to file: " + propertiesJsonFile, e);
        }

        var sidecarFile = getExtensionDir().resolve(SIDE_CAR_FILE);
        try (var propertiesIs = itemStore.readFile(propertiesJsonFile.toString())) {
            // TODO: refactor to use inputStream directly instead of reading all bytes
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] fileBytes = propertiesIs.readAllBytes();
            byte[] checksumBytes = digest.digest(fileBytes);
            String checksum = Hex.encodeHexString(checksumBytes);
            String checksumWithFilename = checksum + "  " + propertiesJsonFile.getFileName().toString() + "\n";
            // Write the checksum to the sidecar file
            itemStore.writeFile(sidecarFile.toString(), new ByteArrayInputStream(checksumWithFilename.getBytes(StandardCharsets.UTF_8)));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to write sidecar file: " + sidecarFile, e);
        }
    }

    public void putProperty(long version, String key, Object value) {
        if (properties == null) {
            throw new IllegalStateException("Properties have not been loaded");
        }
        var versionKey = "v" + version;
        Map<String, Object> versionProperties = properties.getOrDefault(versionKey, new HashMap<>());
        versionProperties.put(key, value);
        properties.put(versionKey, versionProperties);
    }

}
