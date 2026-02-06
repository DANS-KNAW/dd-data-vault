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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import nl.knaw.dans.layerstore.Item.Type;
import nl.knaw.dans.layerstore.ItemStore;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates the packaging-format-registry extension stored under extensions/packaging-format-registry
 * according to the extension documentation.
 */
public class PackagingFormatRegistryValidator {
    private static final String EXTENSION_DIR = "extensions/packaging-format-registry";
    private static final String CONFIG_PATH = EXTENSION_DIR + "/config.json";
    private static final String INVENTORY_PATH = EXTENSION_DIR + "/packaging_format_inventory.json";
    private static final String FORMATS_DIR = EXTENSION_DIR + "/packaging_formats";

    private static final Set<String> ALLOWED_DIGESTS = Set.of("md5", "sha1", "sha256", "sha512");

    private final ItemStore itemStore;
    private final ObjectMapper mapper = new ObjectMapper();

    public PackagingFormatRegistryValidator(@NonNull ItemStore itemStore) {
        this.itemStore = itemStore;
    }

    /**
     * Validates config.json, packaging_format_inventory.json and checks the packaging_formats directories
     * correspond one-to-one with manifest entries and uniqueness of name/version pairs.
     * Throws IllegalStateException with a descriptive message when validation fails.
     */
    public void validate() {
        JsonNode config = readJson(CONFIG_PATH);
        // Schema-based validation first
        new SchemaValidator(itemStore).validate("packaging-format-registry-config.schema.json", config);
        validateConfig(config);

        JsonNode inventory = readJson(INVENTORY_PATH);
        new SchemaValidator(itemStore).validate("packaging_format_inventory.schema.json", inventory);
        Map<String, ManifestEntry> manifest = readManifest(inventory);
        validateManifestAndDirectories(config, manifest);
    }

    private JsonNode readJson(String path) {
        try (var in = itemStore.readFile(path)) {
            return mapper.readTree(in);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read JSON from " + path, e);
        }
    }

    private void validateConfig(JsonNode config) {
        if (config == null || !config.isObject()) {
            throw new IllegalStateException("packaging-format-registry/config.json must be a JSON object");
        }
        var name = getText(config, "extensionName");
        if (!"packaging-format-registry".equals(name)) {
            throw new IllegalStateException("config.json: extensionName must be 'packaging-format-registry'");
        }
        var pfDigest = getText(config, "packagingFormatDigestAlgorithm");
        var invDigest = getText(config, "digestAlgorithm");
        if (pfDigest == null || pfDigest.isBlank()) {
            throw new IllegalStateException("config.json: packagingFormatDigestAlgorithm must be set");
        }
        if (invDigest == null || invDigest.isBlank()) {
            throw new IllegalStateException("config.json: digestAlgorithm must be set");
        }
        if (!ALLOWED_DIGESTS.contains(pfDigest.toLowerCase())) {
            throw new IllegalStateException("config.json: packagingFormatDigestAlgorithm must be one of " + ALLOWED_DIGESTS);
        }
        if (!ALLOWED_DIGESTS.contains(invDigest.toLowerCase())) {
            throw new IllegalStateException("config.json: digestAlgorithm must be one of " + ALLOWED_DIGESTS);
        }
    }

    private Map<String, ManifestEntry> readManifest(JsonNode inventory) {
        if (inventory == null || !inventory.isObject()) {
            throw new IllegalStateException("packaging_format_inventory.json must be a JSON object");
        }
        var manifestNode = inventory.get("manifest");
        if (manifestNode == null || !manifestNode.isObject()) {
            throw new IllegalStateException("packaging_format_inventory.json must contain object 'manifest'");
        }
        Map<String, ManifestEntry> manifest = new HashMap<>();
        Iterator<String> ids = manifestNode.fieldNames();
        while (ids.hasNext()) {
            var id = ids.next();
            var entryNode = manifestNode.get(id);
            if (entryNode == null || !entryNode.isObject()) {
                throw new IllegalStateException("Manifest entry '" + id + "' must be an object");
            }
            var name = getText(entryNode, "name");
            var version = getText(entryNode, "version");
            var summary = getText(entryNode, "summary");
            if (name == null || version == null || summary == null) {
                throw new IllegalStateException("Manifest entry '" + id + "' must contain 'name', 'version', and 'summary'");
            }
            manifest.put(id, new ManifestEntry(id, name, version));
        }
        return manifest;
    }

    private void validateManifestAndDirectories(JsonNode config, Map<String, ManifestEntry> manifest) {
        // 1. Uniqueness of name/version
        Set<String> pairs = new HashSet<>();
        for (ManifestEntry e : manifest.values()) {
            var key = e.name + "\u0000" + e.version;
            if (!pairs.add(key)) {
                throw new IllegalStateException("Duplicate packaging format (name/version) in manifest: " + e.name + "/" + e.version);
            }
        }

        // 2. One-to-one correspondence with packaging_formats directories
        // List directories under FORMATS_DIR via the layered store by reading directory entries from top layer
        Set<String> formatDirs = listDirectories(FORMATS_DIR);
        if (formatDirs.size() != manifest.size()) {
            throw new IllegalStateException("Mismatch between number of manifest entries (" + manifest.size() + ") and packaging_formats directories (" + formatDirs.size() + ")");
        }
        for (String id : manifest.keySet()) {
            if (!formatDirs.contains(id)) {
                throw new IllegalStateException("Missing packaging_formats directory for manifest id: " + id);
            }
        }

        // 3. Check digest algorithm generates the directory names for <name>/<version>
        String algo = getText(config, "packagingFormatDigestAlgorithm");
        for (ManifestEntry e : manifest.values()) {
            String expected = hexDigest(algo, e.name + "/" + e.version);
            if (!expected.equalsIgnoreCase(e.id)) {
                throw new IllegalStateException("Manifest id does not match " + algo + " digest of name/version for: " + e.name + "/" + e.version);
            }
        }
    }

    private Set<String> listDirectories(String pathLike) {
        try {
            return itemStore.listDirectory(pathLike).stream()
                .filter(i -> i.getType() == Type.Directory)
                .map(i -> {
                    String p = i.getPath();
                    int idx = p.lastIndexOf('/');
                    return idx >= 0 ? p.substring(idx + 1) : p;
                })
                .collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to list directories under '" + pathLike + "' for validation", e);
        }
    }

    private String getText(JsonNode node, String field) {
        var v = node.get(field);
        return v != null && v.isTextual() ? v.asText() : null;
    }

    private String hexDigest(String algo, String input) {
        try {
            MessageDigest md = MessageDigest.getInstance(algo.toUpperCase());
            byte[] bytes = md.digest(input.getBytes());
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
        catch (Exception e) {
            throw new IllegalStateException("Unsupported digest algorithm: " + algo, e);
        }
    }

    private record ManifestEntry(String id, String name, String version) {}
}
