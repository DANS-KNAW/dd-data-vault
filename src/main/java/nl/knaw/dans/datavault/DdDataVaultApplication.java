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
import nl.knaw.dans.datavault.core.JobHandler;
import nl.knaw.dans.datavault.core.JobPoller;
import nl.knaw.dans.datavault.core.JobRunner;
import nl.knaw.dans.datavault.core.OcflRepositoryProvider;
import nl.knaw.dans.datavault.db.JobDao;
import nl.knaw.dans.datavault.resources.JobDtoWriter;
import nl.knaw.dans.datavault.resources.JobsApiResource;

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
        var jobDao = new JobDao(hibernateBundle.getSessionFactory());
        environment.jersey().register(new JobsApiResource(jobDao));
        environment.jersey().register(new JobDtoWriter());

        String uuidRegex = "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}";
        JobRunner jobRunner = JobRunner.builder()
            .jobDao(jobDao)
            .repositoryProvider(new OcflRepositoryProvider()) // TODO: make configurable
            .validObjectIdentifierPattern("^urn:nbn:nl:ui:13-" + uuidRegex + "$") // TODO: make configurable
            .executorService(environment.lifecycle().executorService("job-runner-%d").build()) // TODO: make configurable
            .build();

        environment.lifecycle().manage(createUnitOfWorkAwareJobPoller(jobDao, jobRunner));
    }

    private JobPoller createUnitOfWorkAwareJobPoller(JobDao jobDao, JobRunner jobRunner) {
        return new UnitOfWorkAwareProxyFactory(hibernateBundle)
            .create(JobPoller.class, new Class[] { JobDao.class, JobHandler.class }, new Object[] { jobDao, jobRunner });
    }

}
