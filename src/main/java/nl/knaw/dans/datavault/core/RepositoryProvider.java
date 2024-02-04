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

import java.nio.file.Path;

/**
 * Provides access to the repository.
 */
public interface RepositoryProvider {

    /**
     * Adds a new version to the object identified by the given object id. If the object does not exist yet, it will be created.
     *
     * @param objectId               The identifier of the object
     * @param objectVersionDirectory The directory containing the new version of the object
     */
    void addVersion(String objectId, Path objectVersionDirectory);

}
