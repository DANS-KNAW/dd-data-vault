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
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import nl.knaw.dans.layerstore.ItemStore;

import java.util.Map;

/**
 * Validates a candidate object_version_properties.json content against the schema in the OCFL root.
 * The schema filename used is 'object-version-properties.schema.json'.
 */
public class ObjectVersionPropertiesValidator {
    private static final String SCHEMA_FILE = "object-version-properties.schema.json";

    private final ItemStore itemStore;
    private final ObjectMapper mapper = new ObjectMapper();

    public ObjectVersionPropertiesValidator(@NonNull ItemStore itemStore) {
        this.itemStore = itemStore;
    }

    /**
     * Validate the provided properties map against the schema. Throws IllegalArgumentException on failure.
     */
    public void validate(@NonNull Map<String, Map<String, Object>> properties) {
        // Convert the Java Map to a JsonNode and validate using SchemaValidator
        ObjectNode root = mapper.valueToTree(properties);
        try {
            new SchemaValidator(itemStore).validate(SCHEMA_FILE, root);
        }
        catch (IllegalStateException e) {
            // Re-wrap as IllegalArgumentException per contract
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}

