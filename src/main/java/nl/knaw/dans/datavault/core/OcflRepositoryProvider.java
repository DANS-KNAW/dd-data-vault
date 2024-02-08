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
import io.dropwizard.lifecycle.Managed;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.layerstore.ItemStore;
import nl.knaw.dans.lib.ocflext.LayeredStorage;

import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@AllArgsConstructor
public class OcflRepositoryProvider implements RepositoryProvider, Managed {
    private ItemStore itemStore;
    private Path workDir;

    @Override
    @UnitOfWork
    public void addVersion(String objectId, Path objectVersionDirectory) {
        log.debug("Adding version {} to object {}", objectVersionDirectory, objectId);
        // TODO: implement
    }

    @Override
    @SneakyThrows
    @UnitOfWork
    public void start() {
        log.info("Starting OCFL repository provider");
        var layeredStorage = new LayeredStorage(itemStore);
        var layoutConfig = new NTupleOmitPrefixStorageLayoutConfig().setDelimiter(":").setTupleSize(3); // TODO: make configurable
        new OcflRepositoryBuilder()
            .defaultLayoutConfig(layoutConfig)
            .inventoryCache(null)
            .storage(ocflStorageBuilder -> ocflStorageBuilder.storage(layeredStorage))
            .workDir(Files.createDirectories(workDir)).build();
    }
}
