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

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.datavault.config.DdDataVaultConfig;
import nl.knaw.dans.datavault.core.JobServiceImpl;
import nl.knaw.dans.datavault.core.OcflRepositoryProvider;
import nl.knaw.dans.datavault.resources.JobsApiResource;
import nl.knaw.dans.layerstore.ItemStore;
import nl.knaw.dans.layerstore.LayerDatabaseImpl;
import nl.knaw.dans.layerstore.LayerManagerImpl;
import nl.knaw.dans.layerstore.LayeredItemStore;

import java.nio.file.Path;
import java.util.regex.Pattern;

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
    }

    @Override
    public void run(final DdDataVaultConfig configuration, final Environment environment) {
        var validObjectIdentifierPattern = Pattern.compile(configuration.getDataVault().getValidObjectIdentifierPattern());

        var dao = new LayerDatabaseImpl(hibernateBundle.getSessionFactory());
        var layerManager = new LayerManagerImpl(configuration.getDataVault().getLayerStore().getStagingRoot(), configuration.getDataVault().getLayerStore().getArchiveRoot(),
            environment.lifecycle().executorService("archiver-worker").build());
        var itemStore = new LayeredItemStore(dao, layerManager);
        var ocflRepositoryProvider = createUnitOfWorkAwareOcflRepositoryProvider(itemStore, configuration.getDataVault().getOcflRepository().getWorkDir());
        environment.lifecycle().manage(ocflRepositoryProvider);
        var jobService = JobServiceImpl.builder()
            .repositoryProvider(ocflRepositoryProvider)
            .validObjectIdentifierPattern(validObjectIdentifierPattern)
            .createOrUpdateExecutor(configuration.getExecutorService().build(environment))
            .build();
        environment.jersey().register(new JobsApiResource(jobService));
    }

    private OcflRepositoryProvider createUnitOfWorkAwareOcflRepositoryProvider(ItemStore itemStore, Path workDir) {
        return new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(OcflRepositoryProvider.class, new Class<?>[] { ItemStore.class, Path.class }, new Object[] { itemStore, workDir });
    }

}
