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
import nl.knaw.dans.datavault.config.InitChecksConfig;
import nl.knaw.dans.datavault.config.RootExtensionsInitChecksConfig;
import nl.knaw.dans.datavault.db.VaultObjectDao;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(DropwizardExtensionsSupport.class)
public class OcflRepositoryProviderObjectVersionPropertiesTest extends AbstractTestFixture {
    private static final String OBJECT_ID = "urn:nbn:o1";
    private static final String PROPERTIES_FILE = "object-version-properties/object_version_properties.json";
    private static final String SIDECAR_FILE = "object-version-properties/object_version_properties.json.sha512";
    private static final String PACKAGING_FORMAT = "DANS RDA BagPack Profile/0.1.0";

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .addEntityClass(VaultObject.class)
        .build();
    private VaultObjectDao vaultObjectDao;
    private OcflRepositoryProvider provider;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        var rootDocsPath = testDir.resolve("root-docs");
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/extension-docs/").toFile(), rootDocsPath.toFile());
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/schemas/").toFile(), rootDocsPath.toFile());
        var itemStore = new LayeredItemStore.Builder()
            .database(new LayerDatabaseImpl(new PersistenceProviderImpl<>(db.getSessionFactory(), ItemRecord.class)))
            .stagingRoot(createSubdir("layer-staging-root"))
            .archiveProvider(new ZipArchiveProvider(createSubdir("layer-archive-root")))
            .databaseBackedContentManager(new StoreInventoryDbBackedContentManager())
            .build();
        // lockObject uses a PostgreSQL advisory lock, which is not available in H2
        vaultObjectDao = spy(new VaultObjectDao(db.getSessionFactory()));
        doNothing().when(vaultObjectDao).lockObject(anyString());

        provider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(itemStore.getLayerConsistencyChecker())
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve("work-dir"))
            .initChecks(new InitChecksConfig())
            .rootExtensionsInitChecks(new RootExtensionsInitChecksConfig())
            .vaultObjectDao(vaultObjectDao)
            .build();
        provider.start();

        addVersion(1, "simple-object/v1");
        addVersion(2, "simple-object/v2");
    }

    private void addVersion(int version, String source) throws Exception {
        copyToTestDir(source, "test-input");
        Files.writeString(testDir.resolve("test-input/v" + version + ".json"), """
            {
              "version-info": {
                "user": {"name": "Test User", "email": "test.user@mail.com"},
                "message": "Version %d"
              },
              "object-version-properties": {
                "packaging-format": "%s"
              }
            }
            """.formatted(version, PACKAGING_FORMAT));
        db.inTransaction(() -> provider.addVersion(OBJECT_ID, version, testDir.resolve("test-input/v" + version)));
    }

    private String readExtensionFile(String path) throws IOException {
        try (var is = provider.getExtensionFile(OBJECT_ID, path)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private Map<String, Object> deaccessioned() {
        return Map.of("datetime", "2026-08-04T14:37:52Z", "reason", "Invalid dataset");
    }

    @Test
    public void get_returns_properties_of_requested_and_head_version() {
        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v1")))
            .containsExactlyEntriesOf(Map.of("packaging-format", PACKAGING_FORMAT));
        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, null)))
            .containsExactlyEntriesOf(Map.of("packaging-format", PACKAGING_FORMAT));
    }

    @Test
    public void get_throws_NoSuchElementException_for_unknown_object_or_version() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.getObjectVersionProperties("urn:nbn:missing", "v1")))
            .isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v3")))
            .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void set_adds_property_to_requested_version_only_and_updates_sidecar() throws Exception {
        db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned", deaccessioned()));

        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v1")))
            .containsEntry("packaging-format", PACKAGING_FORMAT)
            .containsEntry("deaccessioned", deaccessioned());
        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v2")))
            .doesNotContainKey("deaccessioned");
        var json = readExtensionFile(PROPERTIES_FILE);
        assertThat(readExtensionFile(SIDECAR_FILE))
            .isEqualTo(DigestUtils.sha512Hex(json) + "  object_version_properties.json\n");
        verify(vaultObjectDao).lockObject(OBJECT_ID);
    }

    @Test
    public void set_replaces_existing_value() {
        db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v2", "dataset-version", "1.0"));
        db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v2", "dataset-version", "2.0"));

        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v2")))
            .containsEntry("dataset-version", "2.0");
    }

    @Test
    public void set_rejects_unknown_property_and_leaves_file_unchanged() throws Exception {
        var before = readExtensionFile(PROPERTIES_FILE);

        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "no-such-property", "x")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown property");
        assertThat(readExtensionFile(PROPERTIES_FILE)).isEqualTo(before);
    }

    @Test
    public void set_rejects_value_of_wrong_type() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned", "Invalid dataset")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must be of type object");
    }

    @Test
    public void set_rejects_value_without_required_sub_property() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned",
            Map.of("datetime", "2026-08-04T14:37:52Z"))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required sub-property")
            .hasMessageContaining("reason");
    }

    @Test
    public void set_rejects_value_with_unknown_sub_property() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned",
            Map.of("reason", "Invalid dataset", "reasno", "typo"))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown sub-property");
    }

    @Test
    public void set_throws_NoSuchElementException_for_unknown_version() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v3", "deaccessioned", deaccessioned())))
            .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void set_throws_IllegalStateException_for_blocked_object() {
        db.inTransaction(() -> {
            var object = vaultObjectDao.findByObjectId(OBJECT_ID).orElseThrow();
            object.setBlocked(true);
            vaultObjectDao.save(object);
        });

        assertThatThrownBy(() -> db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned", deaccessioned())))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("blocked");
    }

    @Test
    public void delete_removes_property() {
        db.inTransaction(() -> provider.setObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned", deaccessioned()));

        db.inTransaction(() -> provider.deleteObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned"));

        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v1")))
            .containsExactlyEntriesOf(Map.of("packaging-format", PACKAGING_FORMAT));
    }

    @Test
    public void delete_rejects_removing_required_property() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.deleteObjectVersionProperty(OBJECT_ID, "v1", "packaging-format")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required property");
        assertThat(db.inTransaction(() -> provider.getObjectVersionProperties(OBJECT_ID, "v1")))
            .containsEntry("packaging-format", PACKAGING_FORMAT);
    }

    @Test
    public void delete_throws_NoSuchElementException_for_absent_property() {
        assertThatThrownBy(() -> db.inTransaction(() -> provider.deleteObjectVersionProperty(OBJECT_ID, "v1", "deaccessioned")))
            .isInstanceOf(NoSuchElementException.class);
    }
}
