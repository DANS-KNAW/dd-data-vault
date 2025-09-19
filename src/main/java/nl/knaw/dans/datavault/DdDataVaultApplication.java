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

package nl.knaw.dans.datavault;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.config.DdDataVaultConfig;
import nl.knaw.dans.datavault.core.ConsistencyCheckTaskFactory;
import nl.knaw.dans.datavault.core.ImportBatchTaskFactory;
import nl.knaw.dans.datavault.core.LayerThresholdHandler;
import nl.knaw.dans.datavault.core.OcflRepositoryProvider;
import nl.knaw.dans.datavault.core.PollingTaskExecutor;
import nl.knaw.dans.datavault.core.RepositoryProvider;
import nl.knaw.dans.datavault.core.UnitOfWorkDeclaringLayerConsistencyChecker;
import nl.knaw.dans.datavault.core.UnitOfWorkDeclaringRepositoryProviderAdapter;
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import nl.knaw.dans.datavault.db.ImportBatchDao;
import nl.knaw.dans.datavault.resources.ConsistencyChecksApiResource;
import nl.knaw.dans.datavault.resources.DefaultApiResource;
import nl.knaw.dans.datavault.resources.ImportsApiResource;
import nl.knaw.dans.datavault.resources.LayersApiResource;
import nl.knaw.dans.datavault.resources.ObjectsApiResource;
import nl.knaw.dans.layerstore.ConsistencyCheckingAsyncLayerArchiver;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.ItemsMatchDbConsistencyChecker;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManager;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.lib.ocflext.StoreInventoryDbBackedContentManager;
import nl.knaw.dans.lib.util.PersistenceProviderImpl;

import java.io.IOException;
import java.util.regex.Pattern;

@Slf4j
public class DdDataVaultApplication extends Application<DdDataVaultConfig> {

    private final HibernateBundle<DdDataVaultConfig> hibernateBundle = new DdDataVautHibernateBundle();

    public static void main(final String[] args) throws Exception {
        new DdDataVaultApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Data Vault";
    }

    @Override
    public void initialize(final Bootstrap<DdDataVaultConfig> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
        bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                new EnvironmentVariableSubstitutor(true)));
    }

    @Override
    public void run(final DdDataVaultConfig configuration, final Environment environment) {
        environment.getObjectMapper().registerModule(new JavaTimeModule());
        environment.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        var uowFactory = new UnitOfWorkAwareProxyFactory(hibernateBundle);

        var layerDatabaseDao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(hibernateBundle.getSessionFactory(), ItemRecord.class));
        var layerConsistencyChecker = new ItemsMatchDbConsistencyChecker(layerDatabaseDao);
        var layerManager = createLayerManager(configuration, environment, uowFactory, createUnitOfWorkAwareProxy(uowFactory, layerConsistencyChecker));
        var layeredItemStore = new LayeredItemStore(layerDatabaseDao, layerManager, new StoreInventoryDbBackedContentManager());
        layeredItemStore.setAllowReadingContentFromArchives(false);
        var ocflRepositoryProvider = createUnitOfWorkAwareProxy(uowFactory, OcflRepositoryProvider.builder()
            .itemStore(layeredItemStore)
            .layerConsistencyChecker(layerConsistencyChecker)
            .rootExtensionsSourcePath(configuration.getDataVault().getOcflRepository().getRootExtensionsSourcePath())
            .defaultVersionInfoConfig(configuration.getDataVault().getDefaultVersionInfo())
            .workDir(configuration.getDataVault().getOcflRepository().getWorkDir())
            .build());
        environment.lifecycle().manage(ocflRepositoryProvider);
        var importBatchDao = new ImportBatchDao(hibernateBundle.getSessionFactory());
        environment.jersey().register(new ImportsApiResource(importBatchDao, configuration.getDataVault().getIngest().getInbox()));
        environment.jersey().register(new LayersApiResource(layeredItemStore));
        environment.jersey().register(new ObjectsApiResource(ocflRepositoryProvider));
        environment.jersey().register(new DefaultApiResource());

        var consistencyCheckDao = new ConsistencyCheckDao(hibernateBundle.getSessionFactory());
        environment.jersey().register(new ConsistencyChecksApiResource(consistencyCheckDao));
        environment.lifecycle().manage(createUnitOfWorkAwareProxy(uowFactory,
            new PollingTaskExecutor<>(
                "consistency-checker-task-executor",
                environment.lifecycle().scheduledExecutorService("consistency-checker").build(),
                configuration.getDataVault().getLayerStore().getConsistencyCheckExecutor().getPollingInterval().toJavaDuration(),
                consistencyCheckDao,
                new ConsistencyCheckTaskFactory(consistencyCheckDao, layeredItemStore))));
        environment.lifecycle().manage(createUnitOfWorkAwareProxy(uowFactory,
            new PollingTaskExecutor<>(
                "import-executor-task-executor",
                environment.lifecycle().scheduledExecutorService("import-executor").build(),
                configuration.getDataVault().getIngest().getPollingInterval().toJavaDuration(),
                importBatchDao,
                new ImportBatchTaskFactory(
                    configuration.getDataVault().getIngest().getInbox(),
                    configuration.getDataVault().getIngest().getOutbox(),
                    importBatchDao,
                    environment.lifecycle().executorService("import-worker").build(),
                    ocflRepositoryProvider,
                    Pattern.compile(configuration.getDataVault().getValidObjectIdentifierPattern()),
                    createUnitOfWorkAwareProxy(uowFactory, layeredItemStore, configuration.getDataVault().getLayerStore().getLayerArchivingThreshold().toBytes())))));

    }

    private LayerManager createLayerManager(DdDataVaultConfig configuration, Environment environment, UnitOfWorkAwareProxyFactory uowFactory, LayerConsistencyChecker layerConsistencyChecker) {
        try {
            return new LayerManagerImpl(
                configuration.getDataVault().getLayerStore().getStagingRoot(),
                configuration.getDataVault().getLayerStore().getArchiveProvider().build(),
                new ConsistencyCheckingAsyncLayerArchiver(
                    createUnitOfWorkAwareProxy(uowFactory, layerConsistencyChecker), environment.lifecycle().executorService("archiver-worker").build())
            );
        }
        catch (IOException e) {
            log.error("Error creating LayerManager", e);
            throw new RuntimeException(e);
        }
    }

    private RepositoryProvider createUnitOfWorkAwareProxy(UnitOfWorkAwareProxyFactory uowFactory, RepositoryProvider repositoryProvider) {
        return uowFactory
            .create(UnitOfWorkDeclaringRepositoryProviderAdapter.class, new Class<?>[] { RepositoryProvider.class }, new Object[] { repositoryProvider });
    }

    @SuppressWarnings("unchecked")
    private <R> PollingTaskExecutor<R> createUnitOfWorkAwareProxy(UnitOfWorkAwareProxyFactory uowFactory, PollingTaskExecutor<R> executor) {
        return uowFactory
            .create(PollingTaskExecutor.class, new Class<?>[] { PollingTaskExecutor.class }, new Object[] { executor });
    }

    private UnitOfWorkDeclaringLayerConsistencyChecker createUnitOfWorkAwareProxy(UnitOfWorkAwareProxyFactory uowFactory, LayerConsistencyChecker delegate) {
        return uowFactory
            .create(UnitOfWorkDeclaringLayerConsistencyChecker.class, new Class<?>[] { LayerConsistencyChecker.class },
                new Object[] { delegate });
    }

    private LayerThresholdHandler createUnitOfWorkAwareProxy(UnitOfWorkAwareProxyFactory uowFactory, LayeredItemStore layeredItemStore, long threshold) {
        return uowFactory
            .create(LayerThresholdHandler.class, new Class<?>[] { LayeredItemStore.class, long.class },
                new Object[] { layeredItemStore, threshold });
    }

}