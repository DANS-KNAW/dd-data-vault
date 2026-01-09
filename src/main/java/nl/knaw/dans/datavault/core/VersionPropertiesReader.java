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

import io.ocfl.api.model.User;
import io.ocfl.api.model.VersionInfo;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import org.apache.commons.validator.routines.EmailValidator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

public class VersionPropertiesReader {
    private static final String MAILTO_PREFIX = "mailto:";
    private static final Set<String> VERSION_INFO_KEYS = Set.of("user.name", "user.email", "message");
    private final DefaultVersionInfoConfig defaultConfig;
    private final Properties props;

    public VersionPropertiesReader(Path file, DefaultVersionInfoConfig defaultConfig) throws IOException {
        this.defaultConfig = defaultConfig;

        if (file == null || !Files.exists(file)) {
            if (defaultConfig == null) {
                throw new IllegalArgumentException("No version info file " + file + " provided and no default configuration available");
            }
            this.props = null;
        }
        else {
            this.props = new Properties();
            try (var in = Files.newInputStream(file)) {
                this.props.load(in);
            }

            for (var key : this.props.stringPropertyNames()) {
                if (!VERSION_INFO_KEYS.contains(key) && !key.startsWith("custom.")) {
                    throw new IllegalArgumentException("Unknown property in version info file: " + key);
                }
            }
        }
    }

    public Map<String, String> getCustomProperties() {
        if (this.props == null) {
            return Map.of();
        }

        return this.props.stringPropertyNames().stream()
            .filter(key -> key.startsWith("custom."))
            .collect(Collectors.toMap(
                key -> key.substring("custom.".length()),
                this.props::getProperty
            ));
    }

    public VersionInfo getVersionInfo() {
        if (this.props == null) {
            return createDefaultVersionInfo();
        }

        var info = new VersionInfo();
        var user = new User();
        user.setName(getOrThrow(props, "user.name"));
        var email = getOrThrow(props, "user.email");
        if (!email.startsWith(MAILTO_PREFIX)) {
            email = MAILTO_PREFIX + email;
        }
        validateEmail(email);
        user.setAddress(email);
        info.setUser(user);
        info.setMessage(getOrThrow(props, "message"));
        return info;
    }

    private void validateEmail(String email) {
        var mailWithoutMailTo = email.startsWith(MAILTO_PREFIX) ? email.substring(MAILTO_PREFIX.length()) : email;
        if (!EmailValidator.getInstance().isValid(mailWithoutMailTo)) {
            throw new IllegalArgumentException("Invalid email address: " + email);
        }
    }

    private VersionInfo createDefaultVersionInfo() {
        return new VersionInfo()
            .setMessage(defaultConfig.getMessage())
            .setUser(new User()
                .setName(defaultConfig.getUsername())
                .setAddress(defaultConfig.getEmail().toString()));
    }

    private String getOrThrow(Properties props, String key) {
        var value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return value;
    }
}
