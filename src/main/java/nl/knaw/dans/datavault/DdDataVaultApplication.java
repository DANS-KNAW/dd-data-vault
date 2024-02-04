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
import nl.knaw.dans.datavault.config.DdDataVaultConfiguration;
import nl.knaw.dans.datavault.db.JobDao;
import nl.knaw.dans.datavault.resources.JobsApiResource;

public class DdDataVaultApplication extends Application<DdDataVaultConfiguration> {

    private final HibernateBundle<DdDataVaultConfiguration> hibernateBundle = new DdDataVautHibernateBundle();

    public static void main(final String[] args) throws Exception {
        new DdDataVaultApplication().run(args);
    }

    @Override
    public String getName() {
        return "DD Data Vault";
    }

    @Override
    public void initialize(final Bootstrap<DdDataVaultConfiguration> bootstrap) {
        bootstrap.addBundle(hibernateBundle);
    }

    @Override
    public void run(final DdDataVaultConfiguration configuration, final Environment environment) {
        environment.jersey().register(new JobsApiResource(new JobDao(hibernateBundle.getSessionFactory())));
    }

}
