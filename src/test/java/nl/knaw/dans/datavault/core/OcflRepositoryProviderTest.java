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
import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import nl.knaw.dans.layerstore.DirectLayerArchiver;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.ItemsMatchDbConsistencyChecker;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayerDatabase;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManager;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class OcflRepositoryProviderTest extends AbstractTestFixture {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * The subdirectory under the test directory where the layered OCFL repository keeps its staged layers.
     */
    private static final String LAYER_STAGING_ROOT = "layer-staging-root";

    /**
     * The subdirectory under the test directory where the layered OCFL repository keeps its archived layers.
     */
    private static final String LAYER_ARCHIVE_ROOT = "layer-archive-root";

    /**
     * The subdirectory under the test directory where the OCFL repository keeps its working files.
     */
    private static final String WORK_DIR = "work-dir";

    /**
     * The subdirectory under the test directory where the test input files copied to before running tests.
     */
    private static final String TEST_INPUT = "test-input";

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .build();
    private final LayerDatabase dao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(db.getSessionFactory(), ItemRecord.class));
    private OcflRepositoryProvider ocflRepositoryProvider;
    private LayerManager layerManager;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT);
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT);
        layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());
        var defaultVersionInfoConfig = new DefaultVersionInfoConfig();
        defaultVersionInfoConfig.setUsername("test-user");
        defaultVersionInfoConfig.setEmail(URI.create("mailto:testuser@example.com"));
        defaultVersionInfoConfig.setMessage("Test message");

        ocflRepositoryProvider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(new ItemsMatchDbConsistencyChecker(dao))
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .defaultVersionInfoConfig(defaultVersionInfoConfig)
            .workDir(testDir.resolve(WORK_DIR))
            .build();

        ocflRepositoryProvider.start();

    }

    @Test
    public void addHeadVersion_should_create_new_object() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);

        // When
        ocflRepositoryProvider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"));

        // Then
        long layerId = layerManager.getTopLayer().getId();
        var objectRoot = testDir.resolve(LAYER_STAGING_ROOT).resolve(Long.toString(layerId)).resolve("000/000/0o1/o1");
        assertThat(objectRoot.resolve("v1"))
            .exists()
            .isDirectory();
        assertThat(objectRoot.resolve("v1").resolve("content/file1.txt"))
            .exists()
            .isRegularFile();
        assertThat(objectRoot.resolve("v1").resolve("content/file2.txt"))
            .exists()
            .isRegularFile();
        assertThat(objectRoot.resolve("extensions/object-version-properties/object_version_properties.json"))
            .exists()
            .isRegularFile();
        assertThat(objectRoot.resolve("extensions/object-version-properties/object_version_properties.json.sha512"))
            .exists()
            .isRegularFile();

        // Verify that the object version properties are set correctly
        Map<String, Map<String, Object>> objectVersionProperties = mapper.readValue(
            objectRoot.resolve("extensions/object-version-properties/object_version_properties.json").toFile(),
            mapper.getTypeFactory().constructMapType(Map.class, String.class, Map.class)
        );

        assertThat(objectVersionProperties.keySet()).hasSize(1);
        assertThat(objectVersionProperties).containsKey("v1");
        assertThat(objectVersionProperties.get("v1").keySet()).hasSize(1);
        assertThat(objectVersionProperties.get("v1")).containsEntry(OcflRepositoryProvider.PACKAGING_FORMAT_KEY,
            OcflRepositoryProvider.DANS_RDA_BAG_PACK_PROFILE_0_1_0);

    }


    @Test
    public void addVersion_should_add_version_to_existing_object() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);
        ocflRepositoryProvider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"));
        copyToTestDir("simple-object/v2", TEST_INPUT);

        // When
        ocflRepositoryProvider.addVersion("urn:nbn:o1", 2, testDir.resolve(TEST_INPUT + "/v2"));

        // Then
        long layerId = layerManager.getTopLayer().getId();
        var objectRoot = testDir.resolve(LAYER_STAGING_ROOT).resolve(Long.toString(layerId)).resolve("000/000/0o1/o1");
        assertThat(objectRoot.resolve("v2"))
            .exists()
            .isDirectory();
        // file1.txt should not be present in v2, as it was unchanged and therefore referenced from v1
        assertThat(objectRoot.resolve("v2").resolve("content/file3.txt"))
            .exists()
            .isRegularFile();
        assertThat(objectRoot.resolve("extensions/object-version-properties/object_version_properties.json"))
            .exists()
            .isRegularFile();
        assertThat(objectRoot.resolve("extensions/object-version-properties/object_version_properties.json.sha512"))
            .exists()
            .isRegularFile();

        // Verify that the object version properties are set correctly
        Map<String, Map<String, Object>> objectVersionProperties = mapper.readValue(
            objectRoot.resolve("extensions/object-version-properties/object_version_properties.json").toFile(),
            mapper.getTypeFactory().constructMapType(Map.class, String.class, Map.class)
        );

        assertThat(objectVersionProperties.keySet()).hasSize(2);
        assertThat(objectVersionProperties).containsKey("v1");
        assertThat(objectVersionProperties.get("v1").keySet()).hasSize(1);
        assertThat(objectVersionProperties.get("v1")).containsEntry(OcflRepositoryProvider.PACKAGING_FORMAT_KEY,
            OcflRepositoryProvider.DANS_RDA_BAG_PACK_PROFILE_0_1_0);
        assertThat(objectVersionProperties).containsKey("v2");
        assertThat(objectVersionProperties.get("v2").keySet()).hasSize(1);
        assertThat(objectVersionProperties.get("v2")).containsEntry(OcflRepositoryProvider.PACKAGING_FORMAT_KEY,
            OcflRepositoryProvider.DANS_RDA_BAG_PACK_PROFILE_0_1_0);
    }

    @Test
    public void addHeadVersion_should_add_custom_properties() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var properties = """
            user.name=Test User
            user.email=test.user@mail.com
            message=Initial version
            custom.key1=Value 1
            custom.key2=Value 2
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.properties"), properties);

        // When
        ocflRepositoryProvider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"));

        // Then
        long layerId = layerManager.getTopLayer().getId();
        var objectRoot = testDir.resolve(LAYER_STAGING_ROOT).resolve(Long.toString(layerId)).resolve("000/000/0o1/o1");
        
        // Verify that the object version properties are set correctly
        Map<String, Map<String, Object>> objectVersionProperties = mapper.readValue(
            objectRoot.resolve("extensions/object-version-properties/object_version_properties.json").toFile(),
            mapper.getTypeFactory().constructMapType(Map.class, String.class, Map.class)
        );

        assertThat(objectVersionProperties.get("v1")).containsEntry("key1", "Value 1");
        assertThat(objectVersionProperties.get("v1")).containsEntry("key2", "Value 2");
        assertThat(objectVersionProperties.get("v1")).containsEntry(OcflRepositoryProvider.PACKAGING_FORMAT_KEY,
            OcflRepositoryProvider.DANS_RDA_BAG_PACK_PROFILE_0_1_0);
    }

    @Test
    public void addVersion_should_add_custom_properties() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);
        ocflRepositoryProvider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"));
        copyToTestDir("simple-object/v2", TEST_INPUT);
        var properties = """
            user.name=Test User
            user.email=test.user@mail.com
            message=Second version
            custom.key3=Value 3
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v2.properties"), properties);

        // When
        ocflRepositoryProvider.addVersion("urn:nbn:o1", 2, testDir.resolve(TEST_INPUT + "/v2"));

        // Then
        long layerId = layerManager.getTopLayer().getId();
        var objectRoot = testDir.resolve(LAYER_STAGING_ROOT).resolve(Long.toString(layerId)).resolve("000/000/0o1/o1");

        // Verify that the object version properties are set correctly
        Map<String, Map<String, Object>> objectVersionProperties = mapper.readValue(
            objectRoot.resolve("extensions/object-version-properties/object_version_properties.json").toFile(),
            mapper.getTypeFactory().constructMapType(Map.class, String.class, Map.class)
        );

        assertThat(objectVersionProperties.get("v2")).containsEntry("key3", "Value 3");
        assertThat(objectVersionProperties.get("v2")).containsEntry(OcflRepositoryProvider.PACKAGING_FORMAT_KEY,
            OcflRepositoryProvider.DANS_RDA_BAG_PACK_PROFILE_0_1_0);
    }


    // TODO: sidecar file must have the algorithm as inventory sidecar file (this must then first be made configurable in OcflRepositoryProvider)
}
