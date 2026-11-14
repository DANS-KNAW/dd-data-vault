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

import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;
import nl.knaw.dans.datavault.core.ExternalCommandRunner;
import nl.knaw.dans.datavault.core.LocalDmfTarRunner;
import nl.knaw.dans.datavault.core.MoratoriumManager;
import nl.knaw.dans.datavault.core.RsyncedDmfTarArchiveProvider;
import nl.knaw.dans.layerstore.ArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarArchiveProvider;
import nl.knaw.dans.layerstore.DmfTarRunner;
import nl.knaw.dans.layerstore.SshRunner;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;

@Data
@JsonTypeName("DMFTAR")
public class DmfTarArchiveProviderFactory implements ArchiveProviderFactory {
    @NotNull
    private Path dmfTarExecutable;

    @NotNull
    private Path sshExecutable;

    @NotNull
    private String user;

    @NotNull
    private String host;

    // Directory on the remote host where the archive files are stored
    @NotNull
    private Path archiveRoot;

    // Local directory in which dmftar directories are created before being transferred to the remote host, and into which they are fetched before being unpacked
    @NotNull
    private Path localTransferRoot;

    @Valid
    @NotNull
    private ExternalCommandConfig rsyncUpload = new ExternalCommandConfig("/usr/bin/rsync",
        List.of("-av", "--partial", "--append-verify", "${localDir}/${name}", "${user}@${host}:${remoteTransferDir}/"));

    @Valid
    @NotNull
    private ExternalCommandConfig rsyncDownload = new ExternalCommandConfig("/usr/bin/rsync",
        List.of("-av", "--partial", "--append-verify", "${user}@${host}:${remoteDir}/${name}", "${localDir}/"));

    @Valid
    @NotNull
    private VerifyCommandConfig verify = new VerifyCommandConfig();

    @Valid
    @NotNull
    private RetryConfig retry = new RetryConfig();

    @Valid
    @NotNull
    private MoratoriumConfig moratorium = new MoratoriumConfig();

    /**
     * Creates the command line for running a script on the remote host.
     */
    public ExternalCommandConfig sshCommand(String script) {
        return new ExternalCommandConfig(sshExecutable.toString(), List.of("-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "${user}@${host}", script));
    }

    @Override
    public ArchiveProvider build() {
        var ssh = new SshRunner(sshExecutable, user, host, archiveRoot);
        var remoteProvider = new DmfTarArchiveProvider(new DmfTarRunner(dmfTarExecutable, ssh, user, host, archiveRoot), ssh);
        var moratoriumManager = new MoratoriumManager(moratorium);
        return new RsyncedDmfTarArchiveProvider(remoteProvider, this, new ExternalCommandRunner(moratoriumManager, retry), new LocalDmfTarRunner(dmfTarExecutable), moratoriumManager);
    }
}
