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

import nl.knaw.dans.datavault.config.DmfTarArchiveProviderFactory;
import nl.knaw.dans.layerstore.Archive;
import nl.knaw.dans.layerstore.ArchiveProvider;

import java.io.IOException;
import java.util.List;

/**
 * Provides {@link RsyncedDmfTarArchive}s. Operations on the remote archive root are delegated to the given provider, which also creates the archives that are read from.
 */
public class RsyncedDmfTarArchiveProvider implements ArchiveProvider {
    private final ArchiveProvider remoteProvider;
    private final DmfTarArchiveProviderFactory config;
    private final ExternalCommandRunner commandRunner;
    private final LocalDmfTarRunner localDmfTarRunner;
    private final MoratoriumManager moratoriumManager;

    public RsyncedDmfTarArchiveProvider(ArchiveProvider remoteProvider, DmfTarArchiveProviderFactory config, ExternalCommandRunner commandRunner, LocalDmfTarRunner localDmfTarRunner,
        MoratoriumManager moratoriumManager) {
        this.remoteProvider = remoteProvider;
        this.config = config;
        this.commandRunner = commandRunner;
        this.localDmfTarRunner = localDmfTarRunner;
        this.moratoriumManager = moratoriumManager;
    }

    @Override
    public Archive createArchive(long layerId, boolean exists) {
        return new RsyncedDmfTarArchive(layerId, remoteProvider.createArchive(layerId, exists), exists, config, commandRunner, localDmfTarRunner, moratoriumManager);
    }

    @Override
    public boolean exists(long layerId) {
        moratoriumManager.failIfUnderMoratorium();
        return remoteProvider.exists(layerId);
    }

    @Override
    public List<Long> listLayerIds() throws IOException {
        moratoriumManager.failIfUnderMoratorium();
        return remoteProvider.listLayerIds();
    }

    @Override
    public void validateRoot() throws IOException {
        moratoriumManager.failIfUnderMoratorium();
        remoteProvider.validateRoot();
    }
}
