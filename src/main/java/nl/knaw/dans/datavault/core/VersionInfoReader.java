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
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@RequiredArgsConstructor
public class VersionInfoReader {
    private final DefaultVersionInfoConfig defaultConfig;

    public VersionInfo read(Path file) throws IOException {
        if (file == null || !Files.exists(file)) {
            if (defaultConfig == null) {
                throw new IllegalArgumentException("No version info file " + file + " provided and no default configuration available");
            }

            return createDefaultVersionInfo();
        }

        var props = new Properties();
        try (var in = Files.newInputStream(file)) {
            props.load(in);
        }

        var info = new VersionInfo();
        var user = new User();
        user.setName(getOrThrow(props, "user.name"));
        user.setAddress(getOrThrow(props, "user.email"));
        info.setUser(user);
        info.setMessage(getOrThrow(props, "message"));
        return info;
    }

    private String getOrThrow(Properties props, String key) {
        var value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return value;
    }

    private VersionInfo createDefaultVersionInfo() {
        return new VersionInfo()
            .setMessage(defaultConfig.getMessage())
            .setUser(new User()
                .setName(defaultConfig.getUsername())
                .setAddress(defaultConfig.getEmail().toString())
            );
    }

}
