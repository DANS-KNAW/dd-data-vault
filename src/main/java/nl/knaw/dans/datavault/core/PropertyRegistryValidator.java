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
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import nl.knaw.dans.layerstore.ItemStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Validates object version properties against the property registry that is stored in the storage root
 * at extensions/property-registry/config.json.
 */
public class PropertyRegistryValidator {
    private static final String REGISTRY_PATH = "extensions/property-registry/config.json";
    private static final Set<String> ALLOWED_TYPES = Set.of("number", "string", "boolean", "object");

    private final ItemStore itemStore;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Map of property name to descriptor JSON.
     */
    private final Map<String, JsonNode> registry = new HashMap<>();

    public PropertyRegistryValidator(@NonNull ItemStore itemStore) {
        this.itemStore = itemStore;
        loadRegistry();
    }

    /**
     * Validate the property-registry config.json against the documented rules.
     * Throws IllegalStateException when the configuration is invalid.
     */
    public void validate() {
        try (var in = itemStore.readFile(REGISTRY_PATH)) {
            JsonNode root = mapper.readTree(in);
            if (root == null || !root.isObject()) {
                throw new IllegalStateException("property-registry/config.json must be a JSON object at root");
            }
            var extName = root.get("extensionName");
            if (extName == null || !extName.isTextual() || !"property-registry".equals(extName.asText())) {
                throw new IllegalStateException("property-registry/config.json: extensionName must be 'property-registry'");
            }
            JsonNode registryNode = root.get("propertyRegistry");
            if (registryNode == null || !registryNode.isObject()) {
                throw new IllegalStateException("property-registry/config.json must contain an object 'propertyRegistry'");
            }
            Iterator<String> fieldNames = registryNode.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                JsonNode desc = registryNode.get(key);
                validateDescriptor(key, desc);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read property registry from " + REGISTRY_PATH, e);
        }
    }

    private void validateDescriptor(String key, JsonNode desc) {
        if (desc == null || !desc.isObject()) {
            throw new IllegalStateException("Descriptor for property '" + key + "' must be an object");
        }
        var description = desc.get("description");
        if (description == null || !description.isTextual()) {
            throw new IllegalStateException("Property '" + key + "' must have textual 'description'");
        }
        var typeNode = desc.get("type");
        String type = typeNode != null && typeNode.isTextual() ? typeNode.asText() : "string";
        if (!ALLOWED_TYPES.contains(type)) {
            throw new IllegalStateException("Property '" + key + "' has unsupported type: " + type);
        }
        if ("object".equals(type)) {
            JsonNode propsDesc = desc.get("properties");
            if (propsDesc == null || !propsDesc.isObject()) {
                throw new IllegalStateException("Object-typed property '" + key + "' must declare an object 'properties'");
            }
            // Validate sub descriptors recursively
            Iterator<String> subFields = propsDesc.fieldNames();
            while (subFields.hasNext()) {
                String subKey = subFields.next();
                validateDescriptor(key + "." + subKey, propsDesc.get(subKey));
            }
        }
        var requiredNode = desc.get("required");
        boolean required = requiredNode != null && requiredNode.isBoolean() && requiredNode.asBoolean();
        if (required && desc.has("default")) {
            throw new IllegalStateException("Property '" + key + "' cannot declare a default when required=true");
        }
    }

    /**
     * Validate the given custom properties map against the loaded registry.
     *
     * @param customProperties keys and JSON values to be validated
     * @throws IllegalArgumentException if a property is unknown, required properties are missing, or values have the wrong type/shape
     */
    public void validate(@NonNull Map<String, JsonNode> customProperties) {
        // 1. Check that all provided properties are known and have correct types/shapes
        for (var entry : customProperties.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            var desc = registry.get(key);
            if (desc == null) {
                throw new IllegalArgumentException("Unknown property per property-registry: " + key);
            }
            validateValueAgainstDescriptor(key, value, desc);
        }

        // 2. Check that all required properties are present
        for (var entry : registry.entrySet()) {
            var key = entry.getKey();
            var desc = entry.getValue();
            var requiredNode = desc.get("required");
            boolean required = requiredNode != null && requiredNode.isBoolean() && requiredNode.asBoolean();
            if (required && !customProperties.containsKey(key)) {
                throw new IllegalArgumentException("Missing required property per property-registry: " + key);
            }
        }
    }

    private void validateValueAgainstDescriptor(String path, JsonNode value, JsonNode descriptor) {
        String type = descriptor.hasNonNull("type") ? descriptor.get("type").asText() : "string";
        switch (type) {
            case "string" -> {
                if (!value.isTextual()) {
                    throw new IllegalArgumentException("Property '" + path + "' must be of type string");
                }
            }
            case "number" -> {
                if (!value.isNumber()) {
                    throw new IllegalArgumentException("Property '" + path + "' must be of type number");
                }
            }
            case "boolean" -> {
                if (!value.isBoolean()) {
                    throw new IllegalArgumentException("Property '" + path + "' must be of type boolean");
                }
            }
            case "object" -> {
                if (!value.isObject()) {
                    throw new IllegalArgumentException("Property '" + path + "' must be of type object");
                }
                JsonNode propsDesc = descriptor.get("properties");
                if (propsDesc == null || !propsDesc.isObject()) {
                    throw new IllegalStateException("Registry descriptor for object property '" + path + "' must define an object 'properties'");
                }
                ObjectNode valueObj = (ObjectNode) value;
                // Check for unknown fields in provided value
                Iterator<String> fieldNames = valueObj.fieldNames();
                while (fieldNames.hasNext()) {
                    String subKey = fieldNames.next();
                    if (!propsDesc.has(subKey)) {
                        throw new IllegalArgumentException("Unknown sub-property for '" + path + "': " + subKey);
                    }
                }
                // Check required and validate each known property when present
                Iterator<String> descFieldNames = propsDesc.fieldNames();
                while (descFieldNames.hasNext()) {
                    String subKey = descFieldNames.next();
                    JsonNode subDesc = propsDesc.get(subKey);
                    boolean subRequired = subDesc.has("required") && subDesc.get("required").isBoolean() && subDesc.get("required").asBoolean();
                    JsonNode subValue = valueObj.get(subKey);
                    if (subValue == null) {
                        if (subRequired) {
                            throw new IllegalArgumentException("Missing required sub-property for '" + path + "': " + subKey);
                        }
                        continue;
                    }
                    validateValueAgainstDescriptor(path + "." + subKey, subValue, subDesc);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported property type in registry for '" + path + "': " + type);
        }
    }

    private void loadRegistry() {
        try (var in = itemStore.readFile(REGISTRY_PATH)) {
            JsonNode root = mapper.readTree(in);
            if (root == null || !root.isObject()) {
                throw new IllegalStateException("property-registry/config.json must be a JSON object at root");
            }
            JsonNode registryNode = root.get("propertyRegistry");
            if (registryNode == null || !registryNode.isObject()) {
                throw new IllegalStateException("property-registry/config.json must contain an object 'propertyRegistry'");
            }
            Iterator<String> fieldNames = registryNode.fieldNames();
            while (fieldNames.hasNext()) {
                String key = fieldNames.next();
                registry.put(key, registryNode.get(key));
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read property registry from " + REGISTRY_PATH, e);
        }
    }
}
