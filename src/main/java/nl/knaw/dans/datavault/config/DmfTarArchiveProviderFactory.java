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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import nl.knaw.dans.layerstore.ArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarRunner;
import nl.knaw.dans.layerstore.SshRunner;

import java.nio.file.Path;

@JsonTypeName("DMFTAR")
public class DmfTarArchiveProviderFactory implements ArchiveProviderFactory {
    @JsonProperty
    private Path dmfTarExecutable;

    @JsonProperty
    private Path sshExecutable;

    @JsonProperty
    private String user;

    @JsonProperty
    private String host;
    @JsonProperty
    private Path archiveRoot;

    @Override
    public ArchiveProvider build() {
        return new DmfTarArchiveProvider(
            new DmfTarRunner(dmfTarExecutable, user, host, archiveRoot),
            new SshRunner(sshExecutable, user, host, archiveRoot));
    }
}
