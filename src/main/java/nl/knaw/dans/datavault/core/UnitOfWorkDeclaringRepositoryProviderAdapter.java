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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.AllArgsConstructor;

import java.nio.file.Path;

@AllArgsConstructor
public class UnitOfWorkDeclaringRepositoryProviderAdapter implements RepositoryProvider {
    private final RepositoryProvider delegate;

    @Override
    @UnitOfWork
    public void addVersion(String objectId, int version, Path objectVersionDirectory) {
        delegate.addVersion(objectId, version, objectVersionDirectory);
    }

    @Override
    @UnitOfWork
    public void addHeadVersion(String objectId, Path objectVersionDirectory) {
        delegate.addHeadVersion(objectId, objectVersionDirectory);
    }

    @Override
    @UnitOfWork
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    @UnitOfWork
    public void stop() throws Exception {
        delegate.stop();
    }
}
