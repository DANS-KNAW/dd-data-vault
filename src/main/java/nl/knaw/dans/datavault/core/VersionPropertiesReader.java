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
import io.ocfl.api.model.User;
import io.ocfl.api.model.VersionInfo;
import lombok.NonNull;
import org.apache.commons.validator.routines.EmailValidator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class VersionPropertiesReader {
    private static final String MAILTO_PREFIX = "mailto:";
    private static final Set<String> VERSION_INFO_KEYS = Set.of("version-info", "object-version-properties");

    private final JsonNode root;

    public VersionPropertiesReader(@NonNull Path file) throws IOException {
        if (!Files.exists(file)) {
            throw new IllegalArgumentException("Version info file does not exist: " + file);
        }
        var mapper = new ObjectMapper();
        try (var in = Files.newInputStream(file)) {
            this.root = mapper.readTree(in);
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("Version info JSON must be an object at root");
        }
        // Validate top-level keys
        for (var it = root.fieldNames(); it.hasNext(); ) {
            var key = it.next();
            if (!VERSION_INFO_KEYS.contains(key)) {
                throw new IllegalArgumentException("Unknown property in version info file: " + key);
            }
        }
    }

    public Map<String, JsonNode> getCustomProperties() {
        var customNode = root.get("object-version-properties");
        if (customNode == null) {
            return Map.of();
        }
        if (!customNode.isObject()) {
            throw new IllegalArgumentException("object-version-properties must be a JSON object");
        }
        var fields = customNode.properties().iterator();
        return iterableToStream(fields)
            .collect(Collectors.toMap(Map.Entry::getKey, Entry::getValue));
    }

    public VersionInfo getVersionInfo() {
        var infoNode = root.get("version-info");
        if (infoNode == null) {
            throw new IllegalArgumentException("Missing required property: version-info");
        }
        if (!infoNode.isObject()) {
            throw new IllegalArgumentException("version-info must be a JSON object");
        }
        var userNode = infoNode.get("user");
        if (userNode == null) {
            throw new IllegalArgumentException("Missing required property: version-info.user");
        }
        if (!userNode.isObject()) {
            throw new IllegalArgumentException("version-info.user must be a JSON object");
        }
        var nameNode = userNode.get("name");
        if (nameNode == null) {
            throw new IllegalArgumentException("Missing required property: version-info.user.name");
        }
        var emailNode = userNode.get("email");
        if (emailNode == null) {
            throw new IllegalArgumentException("Missing required property: version-info.user.email");
        }
        var messageNode = infoNode.get("message");
        if (messageNode == null) {
            throw new IllegalArgumentException("Missing required property: version-info.message");
        }

        var info = new VersionInfo();
        var user = new User();
        user.setName(requireText(nameNode, "version-info.user.name"));
        var email = requireText(emailNode, "version-info.user.email");
        if (!email.startsWith(MAILTO_PREFIX)) {
            email = MAILTO_PREFIX + email;
        }
        validateEmail(email);
        user.setAddress(email);
        info.setUser(user);
        info.setMessage(requireText(messageNode, "version-info.message"));
        return info;
    }

    private void validateEmail(String email) {
        var mailWithoutMailTo = email.startsWith(MAILTO_PREFIX) ? email.substring(MAILTO_PREFIX.length()) : email;
        if (!EmailValidator.getInstance().isValid(mailWithoutMailTo)) {
            throw new IllegalArgumentException("Invalid email address: " + email);
        }
    }

    private static String requireText(JsonNode node, String path) {
        if (node == null || node.isNull() || node.asText().isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + path);
        }
        if (!node.isValueNode()) {
            throw new IllegalArgumentException("Property must be a primitive value: " + path);
        }
        return node.asText();
    }

    private static <T> java.util.stream.Stream<Map.Entry<String, JsonNode>> iterableToStream(java.util.Iterator<Map.Entry<String, JsonNode>> it) {
        Iterable<Map.Entry<String, JsonNode>> iterable = () -> it;
        return java.util.stream.StreamSupport.stream(iterable.spliterator(), false);
    }
}
