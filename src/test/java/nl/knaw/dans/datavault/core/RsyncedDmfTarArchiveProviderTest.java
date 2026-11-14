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

import io.dropwizard.util.Duration;
import nl.knaw.dans.datavault.config.DmfTarArchiveProviderFactory;
import nl.knaw.dans.layerstore.ArchiveProvider;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class RsyncedDmfTarArchiveProviderTest {
    private final Path testDir = Path.of("target/test/RsyncedDmfTarArchiveProviderTest").toAbsolutePath();
    private Path remoteRoot;
    private RsyncedDmfTarArchiveProvider provider;

    @BeforeEach
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
        remoteRoot = Files.createDirectories(testDir.resolve("remote"));
        var ssh = testDir.resolve("ssh");
        Files.writeString(ssh, """
            #!/bin/sh
            for a in "$@"; do last=$a; done
            exec sh -c "$last"
            """);
        assertThat(ssh.toFile().setExecutable(true)).isTrue();

        var config = new DmfTarArchiveProviderFactory();
        config.setSshExecutable(ssh);
        config.setDmfTarExecutable(Path.of("/usr/local/bin/dmftar"));
        config.setUser("vault");
        config.setHost("localhost");
        config.setArchiveRoot(remoteRoot);
        config.setLocalTransferRoot(testDir.resolve("local"));
        config.getRetry().setMaxAttempts(1);
        config.getRetry().setBackoff(Duration.milliseconds(1));
        var moratoriumManager = new MoratoriumManager(config.getMoratorium());
        provider = new RsyncedDmfTarArchiveProvider(mock(ArchiveProvider.class), config, new Semaphore(1), new ExternalCommandRunner(moratoriumManager, config.getRetry()),
            new LocalDmfTarRunner(config.getDmfTarExecutable()), moratoriumManager);
    }

    @Test
    public void validateRoot_accepts_empty_root() {
        assertThatCode(provider::validateRoot).doesNotThrowAnyException();
    }

    @Test
    public void validateRoot_accepts_archives_and_partial_transfers() throws Exception {
        Files.createDirectories(remoteRoot.resolve("1700000000000.dmftar"));
        Files.createDirectories(remoteRoot.resolve("1700000000001.dmftar.partial"));

        assertThatCode(provider::validateRoot).doesNotThrowAnyException();
    }

    @Test
    public void validateRoot_rejects_other_entries() throws Exception {
        Files.createDirectories(remoteRoot.resolve("1700000000000.dmftar"));
        Files.createDirectories(remoteRoot.resolve("other.dmftar.partial"));
        Files.createFile(remoteRoot.resolve("1700000000002.dmftar.bak"));

        assertThatThrownBy(provider::validateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("other.dmftar.partial/")
            .hasMessageContaining("1700000000002.dmftar.bak")
            .hasMessageNotContaining("1700000000000.dmftar/");
    }
}
