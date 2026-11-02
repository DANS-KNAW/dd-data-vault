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
import io.dropwizard.util.Duration;
import nl.knaw.dans.layerstore.ArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarArchiveOptions;
import nl.knaw.dans.layerstore.DmfTarArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarRunner;
import nl.knaw.dans.layerstore.MoratoriumManager;
import nl.knaw.dans.layerstore.RsyncRunner;
import nl.knaw.dans.layerstore.SshRunner;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;

@JsonTypeName("DMFTAR")
public class DmfTarArchiveProviderFactory implements ArchiveProviderFactory {
    private static final int SSH_CONNECTION_TIMEOUT_SECONDS = 10;

    @JsonProperty
    private Path dmfTarExecutable;

    @JsonProperty
    private Path sshExecutable;

    @JsonProperty
    private Path rsyncExecutable = Path.of("/usr/bin/rsync");

    @JsonProperty
    private List<String> rsyncArgs = List.of("-av", "--partial", "--append");

    @JsonProperty
    private String user;

    @JsonProperty
    private String host;

    @JsonProperty
    private Path archiveRoot;

    @JsonProperty
    @NotNull
    private Path localPackagingDir;

    @JsonProperty
    private int maxRetries = 10;

    @JsonProperty
    private Duration retryInterval = Duration.minutes(1);

    @JsonProperty
    private boolean verify = true;

    @JsonProperty
    private String invalidOn = "checksum failure";

    @JsonProperty
    @Valid
    private MoratoriumConfig moratorium = new MoratoriumConfig();

    @Override
    public ArchiveProvider build() {
        var moratoriumManager = new MoratoriumManager(moratorium.getTriggerOn(), moratorium.getDuration().toJavaDuration());
        var ssh = new SshRunner(sshExecutable, user, host, archiveRoot, SSH_CONNECTION_TIMEOUT_SECONDS, moratoriumManager);
        var rsync = new RsyncRunner(rsyncExecutable, rsyncArgs, user, host, archiveRoot, moratoriumManager);
        var options = new DmfTarArchiveOptions(localPackagingDir, maxRetries, retryInterval.toJavaDuration(), verify, invalidOn);
        return new DmfTarArchiveProvider(
            new DmfTarRunner(dmfTarExecutable, ssh, rsync, user, host, archiveRoot, moratoriumManager),
            ssh,
            options);
    }
}
