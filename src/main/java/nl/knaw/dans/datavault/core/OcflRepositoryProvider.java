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
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.UnsupportedExtensionBehavior;
import io.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import io.ocfl.core.storage.OcflStorage;
import io.ocfl.core.storage.OcflStorageBuilder;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.api.OcflObjectVersionDto;
import nl.knaw.dans.datavault.config.DefaultVersionInfoConfig;
import nl.knaw.dans.layerstore.ItemStore;
import nl.knaw.dans.lib.ocflext.LayeredStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE) // Builder should be used to create instances
public class OcflRepositoryProvider implements RepositoryProvider, Managed {
    public static final String PACKAGING_FORMAT_KEY = "packaging-format";
    public static final String DANS_RDA_BAG_PACK_PROFILE_0_1_0 = "DANS RDA BagPack Profile/0.1.0";

    @NonNull
    private ItemStore itemStore;

    @NonNull
    private Path workDir;

    @NonNull
    private DefaultVersionInfoConfig defaultVersionInfoConfig;

    private OcflRepository ocflRepository;
    private OcflStorage ocflStorage;

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

        updateObjectVersionProperties(objectId, version, PACKAGING_FORMAT_KEY, DANS_RDA_BAG_PACK_PROFILE_0_1_0);
    }

    @Override
    public void addHeadVersion(String objectId, Path objectVersionDirectory) {
        log.debug("Adding version import directory {} to object {} as head version", objectVersionDirectory, objectId);
        if (ocflRepository == null) {
            throw new IllegalStateException("OCFL repository is not yet started");
        }
        ocflRepository.putObject(ObjectVersionId.head(objectId), objectVersionDirectory, createVersionInfo());
        long headVersion = Optional.ofNullable(ObjectVersionId.head(objectId).getVersionNum()).map(VersionNum::getVersionNum).orElse(1L);

        updateObjectVersionProperties(objectId, headVersion, PACKAGING_FORMAT_KEY, DANS_RDA_BAG_PACK_PROFILE_0_1_0);
    }

    private void updateObjectVersionProperties(String objectId, long version, String key, Object value) {
        var ovp = new ObjectVersionProperties(itemStore, ocflStorage.objectRootPath(objectId));
        try {
            ovp.load();
            ovp.putProperty(version, key, value);
            ovp.save();
            ovp.validate();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to update object version properties", e);
        }
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
        ocflStorage = new OcflStorageBuilder().storage(layeredStorage).build();
        var layoutConfig = new NTupleOmitPrefixStorageLayoutConfig().setDelimiter(":").setTupleSize(3); // TODO: make configurable
        try {
            ocflRepository = new OcflRepositoryBuilder()
                .unsupportedExtensionBehavior(UnsupportedExtensionBehavior.WARN)
                .defaultLayoutConfig(layoutConfig)
                .inventoryCache(null)
                .storage(ocflStorage)
                .workDir(Files.createDirectories(workDir)).build();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create OCFL repository", e);
        }
    }
}
