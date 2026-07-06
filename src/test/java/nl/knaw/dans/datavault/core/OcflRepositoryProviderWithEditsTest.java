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
import nl.knaw.dans.datavault.config.RootExtensionsInitChecksConfig;
import nl.knaw.dans.datavault.config.RootExtensionsInitEdit;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.LayerDatabase;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.apache.commons.io.FileUtils;
import org.hibernate.Session;
import org.hibernate.context.internal.ManagedSessionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class OcflRepositoryProviderWithEditsTest extends AbstractTestFixture {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String LAYER_STAGING_ROOT = "layer-staging-root";
    private static final String LAYER_ARCHIVE_ROOT = "layer-archive-root";
    private static final String WORK_DIR = "work-dir";

    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .build();
    private final LayerDatabase dao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(db.getSessionFactory(), ItemRecord.class));

    private OcflRepositoryProvider newProviderWithEdits(List<RootExtensionsInitEdit> edits) throws Exception {
        var rootDocsPath = testDir.resolve("root-docs");
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/extension-docs/").toFile(), rootDocsPath.toFile());
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/schemas/").toFile(), rootDocsPath.toFile());
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT);
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT);
        var itemStore = new LayeredItemStore.Builder()
            .database(dao)
            .stagingRoot(stagingRoot)
            .archiveProvider(new ZipArchiveProvider(archiveRoot))
            .databaseBackedContentManager(new StoreInventoryDbBackedContentManager())
            .build();
        return OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .layerConsistencyChecker(itemStore.getLayerConsistencyChecker())
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve(WORK_DIR))
            .rootExtensionsInitEdits(edits)
            .initChecks(new nl.knaw.dans.datavault.config.InitChecksConfig())
            .rootExtensionsInitChecks(new RootExtensionsInitChecksConfig())
            .build();
    }

    @Test
    public void start_should_apply_root_extensions_init_edit() throws Exception {
        // Given an isolated store and an edit to set dataset-version.required to true
        var localDb = DAOTestExtension.newBuilder().addEntityClass(ItemRecord.class).build();
        var localDao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(localDb.getSessionFactory(), ItemRecord.class));
        var rootDocsPath = testDir.resolve("root-docs");
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/extension-docs/").toFile(), rootDocsPath.toFile());
        FileUtils.copyDirectory(Path.of("target/dans-ocfl-extensions/schemas/").toFile(), rootDocsPath.toFile());
        var stagingRoot = createSubdir(LAYER_STAGING_ROOT);
        var archiveRoot = createSubdir(LAYER_ARCHIVE_ROOT);
        var localStore = new LayeredItemStore.Builder()
            .database(localDao)
            .stagingRoot(stagingRoot)
            .archiveProvider(new ZipArchiveProvider(archiveRoot))
            .databaseBackedContentManager(new StoreInventoryDbBackedContentManager())
            .build();
        var edit = new RootExtensionsInitEdit();
        edit.setFile("property-registry/config.json");
        edit.setJsonPath("$.propertyRegistry.dataset-version.required");
        edit.setValue(Boolean.TRUE);
        var provider = OcflRepositoryProvider.builder()
            .itemStore(localStore)
            .layerConsistencyChecker(localStore.getLayerConsistencyChecker())
            .rootExtensionsSourcePath(Path.of("src/main/assembly/dist/cfg/ocfl-root-extensions"))
            .rootDocsSourcePath(rootDocsPath)
            .workDir(testDir.resolve(WORK_DIR))
            .rootExtensionsInitEdits(List.of(edit))
            .initChecks(new nl.knaw.dans.datavault.config.InitChecksConfig())
            .rootExtensionsInitChecks(new RootExtensionsInitChecksConfig())
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
        edit.setValue("blah"); // wrong type, should be a boolean
        var provider = newProviderWithEdits(List.of(edit));

        // When / Then: start should throw RuntimeException wrapping IllegalStateException
        var ex = Assertions.assertThrows(RuntimeException.class, () -> provider.start());
        assertThat(ex.getCause()).isInstanceOf(IllegalStateException.class);
        assertThat(ex.getCause().getMessage()).contains("Type mismatch");
    }
}
