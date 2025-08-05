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
import nl.knaw.dans.datavault.core.ImportServiceImpl;
import nl.knaw.dans.datavault.core.OcflRepositoryProvider;
import nl.knaw.dans.datavault.core.RepositoryProvider;
import nl.knaw.dans.datavault.core.UnitOfWorkDeclaringRepositoryProviderAdapter;
import nl.knaw.dans.datavault.resources.DefaultApiResource;
import nl.knaw.dans.datavault.resources.ImportsApiResource;
import nl.knaw.dans.datavault.resources.LayersApiResource;
import nl.knaw.dans.datavault.resources.ObjectsApiResource;
import nl.knaw.dans.layerstore.ItemRecord;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
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
        var validObjectIdentifierPattern = Pattern.compile(configuration.getDataVault().getValidObjectIdentifierPattern());
        environment.getObjectMapper().registerModule(new JavaTimeModule());
        environment.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        var dao = new LayerDatabaseImpl(new PersistenceProviderImpl<>(hibernateBundle.getSessionFactory(), ItemRecord.class));
        try {
            var layerManager = new LayerManagerImpl(
                configuration.getDataVault().getLayerStore().getStagingRoot(),
                configuration.getDataVault().getLayerStore().getArchiveProvider().build(),
                environment.lifecycle().executorService("archiver-worker").build());
            var itemStore = new LayeredItemStore(dao, layerManager, new StoreInventoryDbBackedContentManager());
            var ocflRepositoryProvider = createUnitOfWorkAwareProxy(OcflRepositoryProvider.builder()
                .itemStore(itemStore)
                .defaultVersionInfoConfig(configuration.getDataVault().getDefaultVersionInfo())
                .workDir(configuration.getDataVault().getOcflRepository().getWorkDir())
                .build());
            environment.lifecycle().manage(ocflRepositoryProvider);
            var jobService = ImportServiceImpl.builder()
                .repositoryProvider(createUnitOfWorkAwareProxy(ocflRepositoryProvider))
                .inboxDir(configuration.getDataVault().getIngest().getInbox())
                .outboxDir(configuration.getDataVault().getIngest().getOutbox())
                .validObjectIdentifierPattern(validObjectIdentifierPattern)
                .createOrUpdateExecutor(configuration.getExecutorService().build(environment))
                .build();
            environment.jersey().register(new ImportsApiResource(jobService));
            environment.jersey().register(new LayersApiResource(layerManager));
            environment.jersey().register(new ObjectsApiResource(ocflRepositoryProvider));
            environment.jersey().register(new DefaultApiResource());
        }
        catch (IOException e) {
            log.error("Error creating LayerManager", e);
            throw new RuntimeException(e);
        }
    }

    private RepositoryProvider createUnitOfWorkAwareProxy(RepositoryProvider repositoryProvider) {
        return new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(UnitOfWorkDeclaringRepositoryProviderAdapter.class, new Class<?>[] { RepositoryProvider.class }, new Object[] { repositoryProvider });
    }

}
