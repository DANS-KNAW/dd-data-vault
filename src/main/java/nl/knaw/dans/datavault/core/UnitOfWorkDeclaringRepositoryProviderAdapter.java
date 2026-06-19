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
import nl.knaw.dans.datavault.api.OcflExtensionFileDetailsDto;
import nl.knaw.dans.datavault.api.OcflFileDetailsDto;
import nl.knaw.dans.datavault.api.OcflObjectDetailsDto;
import nl.knaw.dans.datavault.api.OcflObjectVersionDto;
import nl.knaw.dans.datavault.api.OcflVersionDetailsDto;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Delegates to a {@link RepositoryProvider} and wraps its methods in {@link UnitOfWork}s.
 */
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
    public Optional<OcflObjectVersionDto> getOcflObjectVersion(String objectId, int version) {
        return delegate.getOcflObjectVersion(objectId, version);
    }

    @Override
    @UnitOfWork
    public List<String> listObjectIds() {
        return delegate.listObjectIds();
    }

    @Override
    @UnitOfWork
    public Optional<OcflObjectDetailsDto> describeObject(String objectId) {
        return delegate.describeObject(objectId);
    }

    @Override
    @UnitOfWork
    public Optional<OcflVersionDetailsDto> getVersionDetails(String objectId, String versionNumber) {
        return delegate.getVersionDetails(objectId, versionNumber);
    }

    @Override
    @UnitOfWork
    public Optional<List<OcflFileDetailsDto>> listFiles(String objectId, String versionNumber) {
        return delegate.listFiles(objectId, versionNumber);
    }

    @Override
    @UnitOfWork
    public List<OcflExtensionFileDetailsDto> listExtensionFiles(String objectId) {
        return delegate.listExtensionFiles(objectId);
    }

    @Override
    @UnitOfWork
    public InputStream getExtensionFile(String objectId, String path) {
        return delegate.getExtensionFile(objectId, path);
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
