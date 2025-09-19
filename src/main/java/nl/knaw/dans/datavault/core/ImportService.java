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

import nl.knaw.dans.datavault.api.ImportCommandDto;

import java.util.UUID;

public interface ImportService {

    /**
     * Add an import job to the queue.
     *
     * @param command the import command
     * @return the resulting import job
     * @throws InvalidImportException if the import is invalid
     */
    ImportBatchTask addImport(ImportCommandDto command) throws InvalidImportException;

    /**
     * Get an import job by its id. The object returned provides information about the status of the import job.
     *
     * @param id the id of the import job
     * @return the import job
     */
    ImportBatchTask getImport(UUID id);
}
