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
import java.util.List;
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
        config.getMoratorium().setTriggerOn(List.of("Connection refused", "Connection timed? out"));
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

    @Test
    public void exists_returns_true_when_archive_exists() throws Exception {
        Files.createDirectories(remoteRoot.resolve("1700000000000.dmftar"));
        assertThat(provider.exists(1700000000000L)).isTrue();
        assertThat(provider.isUnderMoratorium()).isFalse();
    }

    @Test
    public void exists_returns_false_when_archive_missing() {
        assertThat(provider.exists(1700000000000L)).isFalse();
        assertThat(provider.isUnderMoratorium()).isFalse();
    }

    @Test
    public void exists_triggers_moratorium_on_connection_refusal() throws Exception {
        var ssh = testDir.resolve("ssh");
        Files.writeString(ssh, """
            #!/bin/sh
            echo "ssh: connect to host localhost port 22: Connection refused" >&2
            exit 255
            """);

        assertThatThrownBy(() -> provider.exists(1700000000000L))
            .isInstanceOf(MoratoriumException.class);
        assertThat(provider.isUnderMoratorium()).isTrue();

        // Subsequent call fails fast under moratorium
        assertThatThrownBy(() -> provider.exists(1700000000000L))
            .isInstanceOf(MoratoriumException.class);
    }

    @Test
    public void exists_triggers_moratorium_on_configured_timeout_and_fails_fast() throws Exception {
        var ssh = testDir.resolve("ssh");
        Files.writeString(ssh, "#!/bin/sh\necho 'ssh: Connection timed out' >&2\nexit 255\n");
        assertThatThrownBy(() -> provider.exists(1700000000000L)).isInstanceOf(MoratoriumException.class);
        assertThat(provider.isUnderMoratorium()).isTrue();
        Files.writeString(ssh, "#!/bin/sh\nexit 0\n");
        assertThatThrownBy(() -> provider.exists(1700000000000L)).isInstanceOf(MoratoriumException.class);
    }

    @Test
    public void exists_does_not_trigger_on_unrelated_ssh_failure() throws Exception {
        Files.writeString(testDir.resolve("ssh"), "#!/bin/sh\necho 'Permission denied' >&2\nexit 255\n");
        assertThatThrownBy(() -> provider.exists(1700000000000L)).isInstanceOf(CommandFailedException.class);
        assertThat(provider.isUnderMoratorium()).isFalse();
    }

    @Test
    public void exists_does_not_trigger_on_successful_ssh_output() throws Exception {
        Files.writeString(testDir.resolve("ssh"), "#!/bin/sh\necho 'Connection timed out'\nexit 0\n");
        assertThat(provider.exists(1700000000000L)).isTrue();
        assertThat(provider.isUnderMoratorium()).isFalse();
    }

    @Test
    public void listLayerIds_returns_parsed_layer_ids() throws Exception {
        Files.createDirectories(remoteRoot.resolve("1700000000000.dmftar"));
        Files.createDirectories(remoteRoot.resolve("1700000000001.dmftar"));
        Files.createDirectories(remoteRoot.resolve("1700000000002.dmftar.partial"));
        Files.createFile(remoteRoot.resolve("other.dmftar.bak"));

        assertThat(provider.listLayerIds()).containsExactlyInAnyOrder(1700000000000L, 1700000000001L);
        assertThat(provider.isUnderMoratorium()).isFalse();
    }

    @Test
    public void listLayerIds_triggers_moratorium_on_connection_refusal() throws Exception {
        var ssh = testDir.resolve("ssh");
        Files.writeString(ssh, """
            #!/bin/sh
            echo "ssh: connect to host localhost port 22: Connection refused" >&2
            exit 255
            """);

        assertThatThrownBy(() -> provider.listLayerIds())
            .isInstanceOf(MoratoriumException.class);
        assertThat(provider.isUnderMoratorium()).isTrue();

        // Subsequent call fails fast under moratorium
        assertThatThrownBy(() -> provider.listLayerIds())
            .isInstanceOf(MoratoriumException.class);
    }
}
