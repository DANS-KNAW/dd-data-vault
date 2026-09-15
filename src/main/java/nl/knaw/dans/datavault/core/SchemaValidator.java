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
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import lombok.NonNull;
import nl.knaw.dans.layerstore.ItemStore;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Loads JSON Schemas from the OCFL root's top-level directory and validates JSON documents against them.
 */
public class SchemaValidator {
    private final ItemStore itemStore;
    private final SchemaRegistry schemaRegistry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);

    public SchemaValidator(@NonNull ItemStore itemStore) {
        this.itemStore = itemStore;
    }

    public void validate(String schemaFileName, JsonNode document) {
        var schema = loadSchema(schemaFileName);
        List<Error> messages = schema.validate(document.toString(), InputFormat.JSON);
        if (!messages.isEmpty()) {
            var errors = messages.stream().map(Error::getMessage).collect(Collectors.joining("; "));
            throw new IllegalStateException("Schema validation failed for '" + schemaFileName + "': " + errors);
        }
    }

    private Schema loadSchema(String schemaFileName) {
        try (var in = itemStore.readFile(schemaFileName)) {
            return schemaRegistry.getSchema(in);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to load schema from OCFL root: " + schemaFileName, e);
        }
    }
}

