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
import nl.knaw.dans.layerstore.ItemStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class PropertyRegistryValidatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PropertyRegistryValidator validator;

    @BeforeEach
    void setUp() throws Exception {
        // Minimal valid registry with several properties and nested object type
        String registryJson = """
            {
              "extensionName": "property-registry",
              "propertyRegistry": {
                "title": {
                  "description": "A title string",
                  "type": "string"
                },
                "version": {
                  "description": "A version number",
                  "type": "number",
                  "required": true
                },
                "flags": {
                  "description": "Boolean flag",
                  "type": "boolean"
                },
                "meta": {
                  "description": "Nested meta object",
                  "type": "object",
                  "properties": {
                    "author": {
                      "description": "Author name",
                      "type": "string",
                      "required": true
                    },
                    "pages": {
                      "description": "Number of pages",
                      "type": "number"
                    }
                  }
                }
              }
            }""";

        ItemStore itemStore = Mockito.mock(ItemStore.class);
        when(itemStore.readFile("extensions/property-registry/config.json"))
            .thenAnswer(invocation -> toStream(registryJson));

        validator = new PropertyRegistryValidator(itemStore);
    }

    @Test
    void validate_happyPath_withAllPropertiesProvided() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "title", MAPPER.readTree("""
                "Hello"
                """),
            "version", MAPPER.readTree("1"),
            "flags", MAPPER.readTree("false"),
            "meta", MAPPER.readTree("""
                {"author":"Jane","pages":200}
                """)
        );

        assertThatCode(() -> validator.validate(customProps)).doesNotThrowAnyException();
    }

    @Test
    void validate_unknownProperty_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "unknown", MAPPER.readTree("""
                "x"
                """),
            "version", MAPPER.readTree("2")
        );

        assertThatThrownBy(() -> validator.validate(customProps))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validate_missingRequired_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "title", MAPPER.readTree("""
                "Hello"
                """)
        );

        assertThatThrownBy(() -> validator.validate(customProps))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required")
            .hasMessageContaining("version");
    }

    @Test
    void validate_typeMismatch_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "version", MAPPER.readTree("""
                "not-a-number"
                """)
        );

        assertThatThrownBy(() -> validator.validate(customProps))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be of type number");
    }

    @Test
    void validate_objectSubProperties_rules() throws Exception {
        // Unknown sub-property should fail
        Map<String, JsonNode> withUnknownSub = Map.of(
            "version", MAPPER.readTree("""
                1
                """),
            "meta", MAPPER.readTree("""
                {
                    "author":"Jane", "unknown":true
                }
                """)
        );
        assertThatThrownBy(() -> validator.validate(withUnknownSub))
            .isInstanceOf(IllegalArgumentException.class);

        // Missing required sub-property should fail
        Map<String, JsonNode> missingRequiredSub = Map.of(
            "version", MAPPER.readTree("""
                1
                """),
            "meta", MAPPER.readTree("""
                {
                    "pages":100
                }
                """)
        );
        assertThatThrownBy(() -> validator.validate(missingRequiredSub))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required sub-property")
            .hasMessageContaining("author");

        // Type mismatch in sub-prop should fail
        Map<String, JsonNode> typeMismatchSub = Map.of(
            "version", MAPPER.readTree("""
                1
                """),
            "meta", MAPPER.readTree("""
                {
                    "author":123
                }
                """)
        );
        assertThatThrownBy(() -> validator.validate(typeMismatchSub))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be of type string")
            .hasMessageContaining("author");

        // Happy path for object
        Map<String, JsonNode> ok = Map.of(
            "version", MAPPER.readTree("""
                1
                """),
            "meta", MAPPER.readTree("""
                {
                    "author":"Jane"
                }
                """)
        );
        assertThatCode(() -> validator.validate(ok)).doesNotThrowAnyException();
    }

    private static InputStream toStream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }
}
