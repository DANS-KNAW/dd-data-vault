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
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import nl.knaw.dans.layerstore.ItemRecord;
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
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class OcflStorageProviderTest extends AbstractTestFixture {
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
        layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot));
        var itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());
        var defaultVersionInfoConfig = new DefaultVersionInfoConfig();
        defaultVersionInfoConfig.setUsername("test-user");
        defaultVersionInfoConfig.setEmail(URI.create("mailto:testuser@example.com"));
        defaultVersionInfoConfig.setMessage("Test message");

        ocflRepositoryProvider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
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

    }

}
