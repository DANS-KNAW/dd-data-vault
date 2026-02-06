package nl.knaw.dans.datavault.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.layerstore.ItemStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class PropertyRegistryValidatorTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ItemStore itemStore;

    private PropertyRegistryValidator validator;

    @BeforeEach
    void setUp() throws Exception {
        // Minimal valid registry with several properties and nested object type
        String registryJson = "{\n" +
            "  \"extensionName\": \"property-registry\",\n" +
            "  \"propertyRegistry\": {\n" +
            "    \"title\": {\n" +
            "      \"description\": \"A title string\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"version\": {\n" +
            "      \"description\": \"A version number\",\n" +
            "      \"type\": \"number\",\n" +
            "      \"required\": true\n" +
            "    },\n" +
            "    \"flags\": {\n" +
            "      \"description\": \"Boolean flag\",\n" +
            "      \"type\": \"boolean\"\n" +
            "    },\n" +
            "    \"meta\": {\n" +
            "      \"description\": \"Nested meta object\",\n" +
            "      \"type\": \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"author\": {\n" +
            "          \"description\": \"Author name\",\n" +
            "          \"type\": \"string\",\n" +
            "          \"required\": true\n" +
            "        },\n" +
            "        \"pages\": {\n" +
            "          \"description\": \"Number of pages\",\n" +
            "          \"type\": \"number\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        itemStore = Mockito.mock(ItemStore.class);
        when(itemStore.readFile("extensions/property-registry/config.json"))
            .thenAnswer(invocation -> toStream(registryJson));

        validator = new PropertyRegistryValidator(itemStore);
    }

    @Test
    void validate_happyPath_withAllPropertiesProvided() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "title", MAPPER.readTree("\"Hello\""),
            "version", MAPPER.readTree("1"),
            "flags", MAPPER.readTree("false"),
            "meta", MAPPER.readTree("{\"author\":\"Jane\",\"pages\":200}")
        );

        assertDoesNotThrow(() -> validator.validate(customProps));
    }

    @Test
    void validate_unknownProperty_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "unknown", MAPPER.readTree("\"x\""),
            "version", MAPPER.readTree("2")
        );

        assertThrows(IllegalArgumentException.class, () -> validator.validate(customProps));
    }

    @Test
    void validate_missingRequired_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "title", MAPPER.readTree("\"Hello\"")
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> validator.validate(customProps));
        assertTrue(ex.getMessage().contains("Missing required"));
    }

    @Test
    void validate_typeMismatch_throws() throws Exception {
        Map<String, JsonNode> customProps = Map.of(
            "version", MAPPER.readTree("\"not-a-number\"")
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> validator.validate(customProps));
        assertTrue(ex.getMessage().contains("must be of type number"));
    }

    @Test
    void validate_objectSubProperties_rules() throws Exception {
        // Unknown sub-property should fail
        Map<String, JsonNode> withUnknownSub = Map.of(
            "version", MAPPER.readTree("1"),
            "meta", MAPPER.readTree("{\"author\":\"Jane\",\"unknown\":true}")
        );
        assertThrows(IllegalArgumentException.class, () -> validator.validate(withUnknownSub));

        // Missing required sub-property should fail
        Map<String, JsonNode> missingRequiredSub = Map.of(
            "version", MAPPER.readTree("1"),
            "meta", MAPPER.readTree("{\"pages\":100}")
        );
        IllegalArgumentException exMissing = assertThrows(IllegalArgumentException.class, () -> validator.validate(missingRequiredSub));
        assertTrue(exMissing.getMessage().contains("Missing required sub-property"));

        // Type mismatch in sub-prop should fail
        Map<String, JsonNode> typeMismatchSub = Map.of(
            "version", MAPPER.readTree("1"),
            "meta", MAPPER.readTree("{\"author\":123}")
        );
        IllegalArgumentException exType = assertThrows(IllegalArgumentException.class, () -> validator.validate(typeMismatchSub));
        assertTrue(exType.getMessage().contains("must be of type string"));

        // Happy path for object
        Map<String, JsonNode> ok = Map.of(
            "version", MAPPER.readTree("1"),
            "meta", MAPPER.readTree("{\"author\":\"Jane\"}")
        );
        assertDoesNotThrow(() -> validator.validate(ok));
    }

    // Optional: ensure the configuration-level validate() catches a bad registry when present
    @Test
    void config_validate_detects_wrong_extensionName() throws Exception {
        String badRegistryJson = "{\n" +
            "  \"extensionName\": \"wrong\",\n" +
            "  \"propertyRegistry\": {}\n" +
            "}";
        when(itemStore.readFile("extensions/property-registry/config.json"))
            .thenAnswer(invocation -> toStream(badRegistryJson));

        PropertyRegistryValidator badValidator = new PropertyRegistryValidator(itemStore);
        IllegalStateException ex = assertThrows(IllegalArgumentException.class, badValidator::validate);
        assertTrue(ex.getMessage().contains("extensionName"));
    }

    private static InputStream toStream(String json) {
        return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    }
}

