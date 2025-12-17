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

import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionInfoReaderTest extends AbstractTestFixture {

    @Test
    public void should_read_version_info_from_properties_file() throws Exception {
        // Given
        var properties = """
            user.name=Test User
            user.email=mailto:test.user@mail.com
            message=Initial version
            """;
        var v1Props = testDir.resolve("v1.properties");
        Files.writeString(v1Props, properties);

        // When
        var info = new VersionInfoReader(null).read(v1Props);

        // Then
        assertThat(info.getUser().getName()).isEqualTo("Test User");
        assertThat(info.getUser().getAddress()).isEqualTo("mailto:test.user@mail.com");
        assertThat(info.getMessage()).isEqualTo("Initial version");
    }

    @Test
    public void should_add_mailto_prefix_if_missing_in_email() throws Exception {
        // Given
        var properties = """
            user.name=Test User
            user.email=test.user@mail.com
            message=Initial version
            """;
        var v1Props = testDir.resolve("v1.properties");
        Files.writeString(v1Props, properties);

        // When
        var info = new VersionInfoReader(null).read(v1Props);

        // Then
        assertThat(info.getUser().getName()).isEqualTo("Test User");
        assertThat(info.getUser().getAddress()).isEqualTo("mailto:test.user@mail.com");
        assertThat(info.getMessage()).isEqualTo("Initial version");
    }

    @Test
    public void should_throw_exception_for_invalid_email() throws Exception {
        // Given
        var properties = """
            user.name=Test User
            user.email=invalid-email
            message=Initial version
            """;
        var v1Props = testDir.resolve("v1.properties");
        Files.writeString(v1Props, properties);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(v1Props);
        });
        assertThat(ex.getMessage()).isEqualTo("Invalid email address: mailto:invalid-email");
    }


    @Test
    public void should_use_default_version_info_when_file_missing() throws Exception {
        // Given
        var defaultConfig = new nl.knaw.dans.datavault.config.DefaultVersionInfoConfig();
        defaultConfig.setUsername("Default User");
        defaultConfig.setEmail(new java.net.URI("mailto:default.user@mail.com"));
        defaultConfig.setMessage("Default version message");

        // When
        var info = new VersionInfoReader(defaultConfig).read(null);

        // Then
        assertThat(info.getUser().getName()).isEqualTo("Default User");
        assertThat(info.getUser().getAddress()).isEqualTo("mailto:default.user@mail.com");
        assertThat(info.getMessage()).isEqualTo("Default version message");
    }

    @Test
    public void should_throw_exception_for_unknown_property() throws Exception {
        // Given
        var properties = """
            user.name=Test User
            unknown.property=Some Value
            message=Initial version
            """;

        var v2Props = testDir.resolve("v2.properties");
        Files.writeString(v2Props, properties);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(v2Props);
        });
        assertThat(ex.getMessage()).isEqualTo("Unknown property in version info file: unknown.property");
    }

    @Test
    public void should_throw_exception_for_missing_required_property() throws Exception {
        // Given
        var properties = """
            user.name=Test User
            message=Initial version
            """;

        var v3Props = testDir.resolve("v3.properties");
        Files.writeString(v3Props, properties);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(v3Props);
        });
        assertThat(ex.getMessage()).isEqualTo("Missing required property: user.email");
    }

    @Test
    public void should_throw_exception_when_no_file_and_no_default_config() throws Exception {
        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(null);
        });
        assertThat(ex.getMessage()).isEqualTo("No version info file null provided and no default configuration available");
    }

    @Test
    public void should_throw_exception_for_blank_required_property() throws Exception {
        // Given
        var properties = """
            user.name=
            user.email=test.user@mail.com
            message=Initial version
            """;

        var v4Props = testDir.resolve("v4.properties");
        Files.writeString(v4Props, properties);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(v4Props);
        });

        assertThat(ex.getMessage()).isEqualTo("Missing required property: user.name");
    }

    @Test
    public void should_throw_exception_when_file_does_not_exist_and_no_default_config() throws Exception {
        // Given
        var nonExistentFile = testDir.resolve("nonexistent.properties");

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionInfoReader(null).read(nonExistentFile);
        });
        assertThat(ex.getMessage()).isEqualTo("No version info file " + nonExistentFile + " provided and no default configuration available");
    }
}
