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
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.NonNull;
import nl.knaw.dans.layerstore.ItemStore;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Loads JSON Schemas from the OCFL root's top-level directory and validates JSON documents against them.
 */
public class SchemaValidator {
    private final ItemStore itemStore;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    public SchemaValidator(@NonNull ItemStore itemStore) {
        this.itemStore = itemStore;
    }

    public void validate(String schemaFileName, JsonNode document) {
        JsonSchema schema = loadSchema(schemaFileName);
        Set<ValidationMessage> messages = schema.validate(document);
        if (!messages.isEmpty()) {
            String errors = messages.stream().map(ValidationMessage::getMessage).collect(Collectors.joining("; "));
            throw new IllegalStateException("Schema validation failed for '" + schemaFileName + "': " + errors);
        }
    }

    private JsonSchema loadSchema(String schemaFileName) {
        try (var in = itemStore.readFile(schemaFileName)) {
            JsonNode schemaJson = mapper.readTree(in);
            return schemaFactory.getSchema(schemaJson);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to load schema from OCFL root: " + schemaFileName, e);
        }
    }
}

