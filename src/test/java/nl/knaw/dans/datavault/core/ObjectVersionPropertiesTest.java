package nl.knaw.dans.datavault.core;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.ocfl.core.storage.OcflStorage;
import io.ocfl.core.storage.OcflStorageBuilder;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import nl.knaw.dans.layerstore.ArchiveProvider;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.ItemStore;
import nl.knaw.dans.layerstore.LayerDatabase;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.layerstore.ZipArchiveProvider;
import nl.knaw.dans.lib.ocflext.LayeredStorage;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.Files;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ObjectVersionPropertiesTest extends AbstractTestFixture {
    private final DAOTestExtension db = DAOTestExtension.newBuilder()
        .addEntityClass(ItemRecord.class)
        .build();
    private final LayerDatabase dao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(db.getSessionFactory(), ItemRecord.class));
    private ItemStore itemStore;
    private OcflRepositoryProvider ocflRepositoryProvider;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        var stagingRoot = testDir.resolve("staging");
        Files.createDirectories(stagingRoot);
        var archiveRoot = testDir.resolve("archive");
        Files.createDirectories(archiveRoot);
        var layerManager = new LayerManagerImpl(stagingRoot, new ZipArchiveProvider(archiveRoot));
        itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());
        var defaultVersionInfoConfig = new DefaultVersionInfoConfig();
        defaultVersionInfoConfig.setUsername("test-user");
        defaultVersionInfoConfig.setEmail(URI.create("mailto:testuser@example.com"));
        defaultVersionInfoConfig.setMessage("Test message");

        ocflRepositoryProvider = OcflRepositoryProvider.builder()
            .itemStore(itemStore)
            .defaultVersionInfoConfig(defaultVersionInfoConfig)
            .workDir(testDir)
            .build();

        ocflRepositoryProvider.start();


    }

    @Test
    public void should_save_and_load_properties() throws Exception {
        // Given
        copyToTestDir("simple-object/v1", "o1");

        // When
        ocflRepositoryProvider.addHeadVersion("urn:nbn:o1", testDir.resolve("o1"));

    }

}
