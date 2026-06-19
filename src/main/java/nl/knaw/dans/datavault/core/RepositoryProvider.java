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
import nl.knaw.dans.datavault.api.OcflExtensionFileDetailsDto;
import nl.knaw.dans.datavault.api.OcflFileDetailsDto;
import nl.knaw.dans.datavault.api.OcflObjectDetailsDto;
import nl.knaw.dans.datavault.api.OcflObjectVersionDto;
import nl.knaw.dans.datavault.api.OcflVersionDetailsDto;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Provides access to the repository.
 */
public interface RepositoryProvider extends Managed {

    /**
     * Adds a new version to the object identified by the given object id. If the object does not exist yet, it will be created. Note that the version number must be the next version number in the
     * sequence of versions for the object.
     *
     * @param objectId               The identifier of the object
     * @param version                The version number of the new version
     * @param objectVersionDirectory The directory containing the new version of the object
     */
    void addVersion(String objectId, int version, Path objectVersionDirectory);

    /**
     * Retrieves the version information for the object identified by the given object id and version number.
     *
     * @param objectId The identifier of the object
     * @param version  The version number of the object
     * @return the version information for the object
     */
    Optional<OcflObjectVersionDto> getOcflObjectVersion(String objectId, int version);

    /**
     * Lists all object IDs in the vault.
     *
     * @return a list of object IDs
     */
    List<String> listObjectIds();

    /**
     * Gets a detailed overview of an object.
     *
     * @param objectId The identifier of the object
     * @return the object details
     */
    Optional<OcflObjectDetailsDto> describeObject(String objectId);

    /**
     * Returns the details of a specific version of an object.
     *
     * @param objectId      The identifier of the object
     * @param versionNumber The version number or "latest"
     * @return the version details
     */
    Optional<OcflVersionDetailsDto> getVersionDetails(String objectId, String versionNumber);

    /**
     * Returns the files in a specific version of an object.
     *
     * @param objectId      The identifier of the object
     * @param versionNumber The version number or "latest"
     * @return the files in the specified version
     */
    Optional<List<OcflFileDetailsDto>> listFiles(String objectId, String versionNumber);

    /**
     * Lists all files below the OCFL object's `extensions/` directory.
     *
     * @param objectId The identifier of the object
     * @return the extension files
     * @throws NoSuchElementException when the object does not exist
     */
    List<OcflExtensionFileDetailsDto> listExtensionFiles(String objectId) throws NoSuchElementException;

    /**
     * Returns the content of a specific extension file (path is relative to `extensions/`).
     *
     * @param objectId The identifier of the object
     * @param path     The path to the extension file
     * @return the content of the extension file
     * @throws NoSuchElementException when the object or the file does not exist
     */
    InputStream getExtensionFile(String objectId, String path) throws NoSuchElementException;
}
