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
package nl.knaw.dans.datavault.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.dropwizard.jackson.Discoverable;
import nl.knaw.dans.layerstore.ArchiveProvider;

// Based on https://www.dropwizard.io/en/stable/manual/configuration.html#polymorphic-configuration. However, I could not get
// discoverability to work, so we are using explicit registration of subtypes.
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ZipArchiveProviderFactory.class, name = "ZIP"),
        @JsonSubTypes.Type(value = TarArchiveProviderFactory.class, name = "TAR")
})
public interface ArchiveProviderFactory extends Discoverable {
    ArchiveProvider build();
}
