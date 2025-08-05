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

import io.dropwizard.lifecycle.Managed;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.User;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.api.OcflObjectVersionDto;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import nl.knaw.dans.layerstore.ItemStore;
import nl.knaw.dans.lib.ocflext.LayeredStorage;

import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE) // Builder should be used to create instances
public class OcflRepositoryProvider implements RepositoryProvider, Managed {
    @NonNull
    private ItemStore itemStore;

    @NonNull
    private Path workDir;

    @NonNull
    private DefaultVersionInfoConfig defaultVersionInfoConfig;

    private OcflRepository ocflRepository;

    @Builder
    public static OcflRepositoryProvider create(ItemStore itemStore, Path workDir, DefaultVersionInfoConfig defaultVersionInfoConfig) {
        return new OcflRepositoryProvider(itemStore, workDir, defaultVersionInfoConfig);
    }

    // TODO: add user name and email and message to the method
    @Override
    public void addVersion(String objectId, int version, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as version v{}", objectVersionDirectory, objectId, version);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        // putObject wants the version number of HEAD, so we need to subtract 1 from the version number
        ocflRepository.putObject(ObjectVersionId.version(objectId, version - 1), objectVersionDirectory, createVersionInfo());
    }

    @Override
    public void addHeadVersion(String objectId, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as head version", objectVersionDirectory, objectId);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        ocflRepository.putObject(ObjectVersionId.head(objectId), objectVersionDirectory, createVersionInfo());
    }

    @Override
    public OcflObjectVersionDto getOcflObjectVersion(String objectId, int version) {
        var versionInfo = ocflRepository.getObject(ObjectVersionId.version(objectId, version));
        return new OcflObjectVersionDto()
            .versionNumber(version).created(versionInfo.getCreated());
    }

    private VersionInfo createVersionInfo() {
        return new VersionInfo()
            .setMessage(defaultVersionInfoConfig.getMessage())
            .setUser(new User()
                .setName(defaultVersionInfoConfig.getUsername())
                .setAddress(defaultVersionInfoConfig.getEmail().toString())
            );
    }

    @Override
    public void start() {
        log.info("Starting OCFL repository provider");
        var layeredStorage = new LayeredStorage(itemStore);
        var layoutConfig = new NTupleOmitPrefixStorageLayoutConfig().setDelimiter(":").setTupleSize(3); // TODO: make configurable
        try {
            ocflRepository = new OcflRepositoryBuilder()
                .defaultLayoutConfig(layoutConfig)
                .inventoryCache(null)
                .storage(ocflStorageBuilder -> ocflStorageBuilder.storage(layeredStorage))
                .workDir(Files.createDirectories(workDir)).build();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create OCFL repository", e);
        }
    }
}
