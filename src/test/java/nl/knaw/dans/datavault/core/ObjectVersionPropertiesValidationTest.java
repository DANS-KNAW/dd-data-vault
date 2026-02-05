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

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.layerstore.DirectLayerArchiver;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.ItemsMatchDbConsistencyChecker;
import nl.knaw.dans.layerstore.LayerDatabase;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ObjectVersionPropertiesValidationTest extends AbstractTestFixture {

    private static final String LAYER_STAGING_ROOT = "layer-staging-root";
    private static final String LAYER_ARCHIVE_ROOT = "layer-archive-root";
    private static final String WORK_DIR = "work-dir";
    private static final String TEST_INPUT = "test-input";

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .build();
    private final LayerDatabase dao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(db.getSessionFactory(), ItemRecord.class));

    private OcflRepositoryProvider provider;

    @BeforeEach
    public void setUpEach() throws Exception {
        super.setUp();
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT);
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot), new DirectLayerArchiver());
        var itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());
        provider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(new ItemsMatchDbConsistencyChecker(dao))
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .workDir(testDir.resolve(WORK_DIR))
            .build();
        provider.start();
    }

    @Test
    public void should_fail_on_unknown_property() throws Exception {
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var json = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "packaging-format": "DANS RDA BagPack Profile/0.1.0",
                "unknown-key": "Value"
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), json);

        var ex = assertThrows(IllegalArgumentException.class, () ->
            provider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"))
        );
        assertThat(ex.getMessage()).contains("Unknown property per property-registry: unknown-key");
    }

    @Test
    public void should_fail_when_required_property_missing() throws Exception {
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var json = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "dataset-version": "1.0"
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), json);

        var ex = assertThrows(IllegalArgumentException.class, () ->
            provider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"))
        );
        assertThat(ex.getMessage()).contains("Missing required property per property-registry: packaging-format");
    }

    @Test
    public void should_fail_when_wrong_type_provided() throws Exception {
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var json = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "packaging-format": 42
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), json);

        var ex = assertThrows(IllegalArgumentException.class, () ->
            provider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"))
        );
        assertThat(ex.getMessage()).contains("Property 'packaging-format' must be of type string");
    }

    @Test
    public void should_fail_when_object_subproperty_missing() throws Exception {
        copyToTestDir("simple-object/v1", TEST_INPUT);
        var json = """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Initial version"
              },
              "object-version-properties": {
                "packaging-format": "DANS RDA BagPack Profile/0.1.0",
                "deaccessioned": {"datetime": "2030-01-01T00:00:00"}
              }
            }
            """;
        Files.writeString(testDir.resolve(TEST_INPUT + "/v1.json"), json);

        var ex = assertThrows(IllegalArgumentException.class, () ->
            provider.addHeadVersion("urn:nbn:o1", testDir.resolve(TEST_INPUT + "/v1"))
        );
        assertThat(ex.getMessage()).contains("Missing required sub-property for 'deaccessioned': reason");
    }
}
