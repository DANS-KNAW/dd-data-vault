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
import nl.knaw.dans.layerstore.ItemStore;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class SchemaValidatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static InputStream toStream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void validate_should_succeed_when_document_matches_schema() throws Exception {
        var schemaJson = """
            {
              "$schema": "https://json-schema.org/draft/2020-12/schema",
              "type": "object",
              "properties": {
                "name": { "type": "string" },
                "age": { "type": "integer" }
              },
              "required": ["name"]
            }
            """;
        var docJson = """
            {
              "name": "Alice",
              "age": 30
            }
            """;

        var itemStore = Mockito.mock(ItemStore.class);
        when(itemStore.readFile("test-schema.json")).thenAnswer(inv -> toStream(schemaJson));

        var validator = new SchemaValidator(itemStore);
        var doc = MAPPER.readTree(docJson);

        assertThatCode(() -> validator.validate("test-schema.json", doc))
            .doesNotThrowAnyException();
    }

    @Test
    void validate_should_throw_when_document_does_not_match_schema() throws Exception {
        var schemaJson = """
            {
              "$schema": "https://json-schema.org/draft/2020-12/schema",
              "type": "object",
              "properties": {
                "name": { "type": "string" },
                "age": { "type": "integer" }
              },
              "required": ["name"]
            }
            """;
        var docJson = """
            {
              "age": "invalid"
            }
            """;

        var itemStore = Mockito.mock(ItemStore.class);
        when(itemStore.readFile("test-schema.json")).thenAnswer(inv -> toStream(schemaJson));

        var validator = new SchemaValidator(itemStore);
        var doc = MAPPER.readTree(docJson);

        assertThatThrownBy(() -> validator.validate("test-schema.json", doc))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Schema validation failed for 'test-schema.json':");
    }

    @Test
    void validate_should_throw_when_schema_cannot_be_loaded() throws Exception {
        var itemStore = Mockito.mock(ItemStore.class);
        when(itemStore.readFile("missing-schema.json")).thenThrow(new IOException("File not found"));

        var validator = new SchemaValidator(itemStore);
        var doc = MAPPER.readTree("{}");

        assertThatThrownBy(() -> validator.validate("missing-schema.json", doc))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Failed to load schema from OCFL root: missing-schema.json");
    }
}
