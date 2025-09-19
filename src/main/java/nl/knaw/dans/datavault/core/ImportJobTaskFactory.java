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

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.datavault.db.ImportJobDao;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

@RequiredArgsConstructor
public class ImportJobTaskFactory implements TaskFactory<ImportJob> {
    private final Path inbox;
    private final Path outbox;
    private final ImportJobDao importJobDao;
    private final ExecutorService executorService;
    private final RepositoryProvider repositoryProvider;
    private final Pattern validObjectIdentifierPattern;
    private final LayerThresholdHandler layerThresholdHandler;

    @Override
    public Runnable create(ImportJob record) {
        var batchOutbox = outbox.resolve(record.getPath());
        initializeBatchOutbox(batchOutbox);
        return new ImportJobTask(
            record.getId(),
            inbox.resolve(record.getPath()),
            batchOutbox,
            importJobDao,
            executorService,
            repositoryProvider,
            validObjectIdentifierPattern,
            layerThresholdHandler);
    }

    private void initializeBatchOutbox(Path batchOutbox) {
        if (!Files.exists(batchOutbox)) {
            try {
                Files.createDirectories(batchOutbox.resolve("processed"));
                Files.createDirectories(batchOutbox.resolve("failed"));
            }
            catch (Exception e) {
                throw new RuntimeException(String.format("Failed to create outbox directory '%s'", batchOutbox), e);
            }
        }
    }
}
