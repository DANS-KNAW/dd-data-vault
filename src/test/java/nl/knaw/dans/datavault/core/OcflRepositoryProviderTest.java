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
import nl.knaw.dans.datavault.config.RootExtensionsInitEdit;
import nl.knaw.dans.layerstore.DirectLayerArchiver;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.ItemsMatchDbConsistencyChecker;
import nl.knaw.dans.layerstore.LayerDatabase;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManager;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.apache.commons.io.FileUtils;
import org.hibernate.Session;
import org.hibernate.context.internal.ManagedSessionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
    private LayerManager layerManagerEdits;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        var rootDocsPath = testDir.resolve("root-docs");
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/extension-docs/").toFile(), rootDocsPath.toFile());
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/schemas/").toFile(), rootDocsPath.toFile());
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT);
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT);
        layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());

        ocflRepositoryProvider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(new ItemsMatchDbConsistencyChecker(dao))
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve(WORK_DIR))
            .build();

        ocflRepositoryProvider.start();

    }

    private OcflRepositoryProvider newProviderWithEdits(List<RootExtensionsInitEdit> edits) throws Exception {
        var rootDocsPath = testDir.resolve("root-docs");
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/extension-docs/").toFile(), rootDocsPath.toFile());
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/schemas/").toFile(), rootDocsPath.toFile());
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT + "-edits");
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT + "-edits");
        layerManagerEdits = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var itemStore = new LayeredItemStore(dao, layerManagerEdits, new StoreInventoryDbBackedContentManager());
        return OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(new ItemsMatchDbConsistencyChecker(dao))
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve(WORK_DIR))
            .rootExtensionsInitEdits(edits)
            .build();
    }

    @Test
    public void addVersion_should_create_new_object() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var json = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "packaging-format": "DANS RDA BagPack Profile/0.1.0"
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), json);

        // When
        ocflRepositoryProvider.addVersion("urn:nbn:o1", 1, testDir.resolve(TEST_INPUT + "/v1"));

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
        assertThat(objectVersionProperties.get("v1")).containsEntry("packaging-format", "DANS RDA BagPack Profile/0.1.0");
    }

    @Test
    public void addVersion_should_add_version_to_existing_object() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var jsonV1 = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "packaging-format": "DANS RDA BagPack Profile/0.1.0"
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), jsonV1);
        ocflRepositoryProvider.addVersion("urn:nbn:o1", 1, testDir.resolve(TEST_INPUT + "/v1"));
        copyToTestDir("simple-object/v2", TEST_INPUT);
        var jsonV2 = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Version 2"
              },
              "object-version-properties": {
                "packaging-format": "DANS RDA BagPack Profile/0.1.0"
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v2.json"), jsonV2);

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
        assertThat(objectVersionProperties.get("v1")).containsEntry("packaging-format", "DANS RDA BagPack Profile/0.1.0");
        assertThat(objectVersionProperties).containsKey("v2");
        assertThat(objectVersionProperties.get("v2").keySet()).hasSize(1);
        assertThat(objectVersionProperties.get("v2")).containsEntry("packaging-format", "DANS RDA BagPack Profile/0.1.0");
    }

    @Test
    public void start_should_apply_root_extensions_init_edit() throws Exception {
        // Given an isolated store and an edit to set dataset-version.required to true
        var localDb = DAOTestExtension.newBuilder().addEntityClass(ItemRecord.class).build();
        var localDao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(localDb.getSessionFactory(), ItemRecord.class));
        var rootDocsPath = testDir.resolve("root-docs");
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT + "-edits");
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT + "-edits");
        var localLayerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var localStore = new LayeredItemStore(localDao, localLayerManager, new StoreInventoryDbBackedContentManager());
        var edit = new RootExtensionsInitEdit();
        edit.setFile("property-registry/config.json");
        edit.setJsonPath("$.propertyRegistry.dataset-version.required");
        edit.setValue(Boolean.TRUE);
        var provider = OcflRepositoryProvider.builder()
            .itemStore(localStore)
            .layerConsistencyChecker(new ItemsMatchDbConsistencyChecker(localDao))
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve(WORK_DIR))
            .rootExtensionsInitEdits(List.of(edit))
            .build();

        // When
        Session session = localDb.getSessionFactory().openSession();
        try {
            ManagedSessionContext.bind(session);
            var tx = session.beginTransaction();
            provider.start();
            tx.commit();
        }
        finally {
            ManagedSessionContext.unbind(localDb.getSessionFactory());
            session.close();
        }

        // Then: verify via the isolated layered item store API
        Session verifySession = localDb.getSessionFactory().openSession();
        try {
            ManagedSessionContext.bind(verifySession);
            var tx2 = verifySession.beginTransaction();
            assertThat(localStore.existsPathLike("extensions/property-registry/config.json")).isTrue();
            try (InputStream is = localStore.readFile("extensions/property-registry/config.json")) {
                var tree = mapper.readTree(is);
                assertThat(tree.path("propertyRegistry").path("dataset-version").path("required").asBoolean()).isTrue();
            }
            tx2.commit();
        }
        finally {
            ManagedSessionContext.unbind(localDb.getSessionFactory());
            verifySession.close();
        }
    }

    @Test
    public void start_should_fail_on_type_mismatch_in_root_extensions_init() throws Exception {
        // Given: editing a boolean with a string value should fail
        var edit = new RootExtensionsInitEdit();
        edit.setFile("property-registry/config.json");
        edit.setJsonPath("$.propertyRegistry.dataset-version.required");
        edit.setValue("true"); // wrong type
        var provider = newProviderWithEdits(List.of(edit));

        // When / Then: start should throw RuntimeException wrapping IllegalStateException
        var ex = Assertions.assertThrows(RuntimeException.class, () -> provider.start());
        assertThat(ex.getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(ex.getCause().getMessage()).contains("Type mismatch");
    }

    // TODO: sidecar file must have the algorithm as inventory sidecar file (this must then first be made configurable in OcflRepositoryProvider)
}
