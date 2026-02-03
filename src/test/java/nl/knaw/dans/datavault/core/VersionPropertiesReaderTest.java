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

public class VersionPropertiesReaderTest extends AbstractTestFixture {

    @Test
    public void should_read_version_info_from_properties_file() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User",
                        "email": "test.user@mail.com"
                     },
                     "message": "Initial version"
               }
            }
            """;
        var v1Json = testDir.resolve("v1.json");
        Files.writeString(v1Json, json);

        // When
        var info = new VersionPropertiesReader(v1Json).getVersionInfo();

        // Then
        assertThat(info.getUser().getName()).isEqualTo("Test User");
        assertThat(info.getUser().getAddress()).isEqualTo("mailto:test.user@mail.com");
        assertThat(info.getMessage()).isEqualTo("Initial version");
    }

    @Test
    public void should_add_mailto_prefix_if_missing_in_email() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User",
                        "email": "test.user@mail.com"
                     },
                     "message": "Initial version"
               }
            }
            """;
        var v1Json = testDir.resolve("v1.json");
        Files.writeString(v1Json, json);

        // When
        var info = new VersionPropertiesReader(v1Json).getVersionInfo();

        // Then
        assertThat(info.getUser().getName()).isEqualTo("Test User");
        assertThat(info.getUser().getAddress()).isEqualTo("mailto:test.user@mail.com");
        assertThat(info.getMessage()).isEqualTo("Initial version");
    }

    @Test
    public void should_throw_exception_for_invalid_email() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User",
                        "email": "invalid-email"
                     },
                     "message": "Initial version"
               }
            }
            """;
        var v1Json = testDir.resolve("v1.json");
        Files.writeString(v1Json, json);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionPropertiesReader(v1Json).getVersionInfo();
        });
        assertThat(ex.getMessage()).isEqualTo("Invalid email address: mailto:invalid-email");
    }


    @Test
    public void should_allow_custom_properties_and_return_them_without_prefix() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User",
                        "email": "test.user@mail.com"
                     },
                     "message": "Initial version"
               },
               "object-version-properties": {
                    "property1": "Value 1",
                    "other-property": "Value 2"
               }
            }
            """;
        var vJson = testDir.resolve("v-custom.json");
        Files.writeString(vJson, json);

        // When
        var reader = new VersionPropertiesReader(vJson);
        var customProps = reader.getCustomProperties();

        // Then
        assertThat(customProps).hasSize(2);
        assertThat(customProps.get("property1")).isEqualTo("Value 1");
        assertThat(customProps.get("other-property")).isEqualTo("Value 2");
    }

    @Test
    public void should_return_empty_map_for_custom_properties_when_none_present() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User",
                        "email": "test.user@mail.com"
                     },
                     "message": "Initial version"
               }
            }
            """;
        var vJson = testDir.resolve("v-no-custom.json");
        Files.writeString(vJson, json);

        // When
        var reader = new VersionPropertiesReader(vJson);
        var customProps = reader.getCustomProperties();

        // Then
        assertThat(customProps).isEmpty();
    }

    @Test
    public void should_throw_exception_for_unknown_property() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User"
                     },
                     "message": "Initial version"
               },
               "unknown.property": "Some Value"
            }
            """;

        var v2Json = testDir.resolve("v2.json");
        Files.writeString(v2Json, json);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionPropertiesReader(v2Json).getVersionInfo();
        });
        assertThat(ex.getMessage()).isEqualTo("Unknown property in version info file: unknown.property");
    }

    @Test
    public void should_throw_exception_for_missing_required_property() throws Exception {
        // Given
        var json = """
            { 
               "version-info" : {
                     "user": {
                        "name": "Test User"
                     },
                     "message": "Initial version"
               }
            }
            """;

        var v3Json = testDir.resolve("v3.json");
        Files.writeString(v3Json, json);

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionPropertiesReader(v3Json).getVersionInfo();
        });
        assertThat(ex.getMessage()).isEqualTo("Missing required property: version-info.user.email");
    }

    @Test
    public void should_throw_exception_when_no_file_provided() throws Exception {
        var ex = assertThrows(NullPointerException.class, () -> {
            new VersionPropertiesReader(null).getVersionInfo();
        });
    }

    @Test
    public void should_throw_exception_when_file_does_not_exist() throws Exception {
        // Given
        var nonExistentFile = testDir.resolve("nonexistent.json");

        // When / Then
        var ex = assertThrows(IllegalArgumentException.class, () -> {
            new VersionPropertiesReader(nonExistentFile).getVersionInfo();
        });
        assertThat(ex.getMessage()).isEqualTo("Version info file does not exist: " + nonExistentFile);
    }
}
