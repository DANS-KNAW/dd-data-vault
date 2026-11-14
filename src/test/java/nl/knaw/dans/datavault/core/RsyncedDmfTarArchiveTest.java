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
import nl.knaw.dans.datavault.config.ExternalCommandConfig;
import nl.knaw.dans.layerstore.Archive;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class RsyncedDmfTarArchiveTest {
    private static final long LAYER_ID = 1700000000000L;
    private static final String NAME = LAYER_ID + ".dmftar";

    private Path testDir;
    private Path binDir;
    private Path stagingDir;
    private Path localTransferRoot;
    private Path remoteRoot;
    private DmfTarArchiveProviderFactory config;
    private MoratoriumManager moratoriumManager;

    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        testDir = Path.of("target/test/RsyncedDmfTarArchiveTest").resolve(testInfo.getTestMethod().orElseThrow().getName()).toAbsolutePath();
        FileUtils.deleteDirectory(testDir.toFile());
        binDir = Files.createDirectories(testDir.resolve("bin"));
        stagingDir = Files.createDirectories(testDir.resolve("staging").resolve(Long.toString(LAYER_ID)));
        Files.writeString(stagingDir.resolve("data.txt"), "content");
        localTransferRoot = testDir.resolve("local-transfer");
        remoteRoot = Files.createDirectories(testDir.resolve("remote"));

        // Fake commands that act on the local file system. Failures are injected by putting a number in bin/rsync.fail, or a file bin/verify.invalid
        var dmftar = script("dmftar", """
            echo run >> "$(dirname "$0")/dmftar.calls"
            mkdir -p "$2" && echo volume > "$2/0000-volume"
            """);
        var rsync = script("rsync", """
            d=$(dirname "$0")
            n=$(cat "$d/rsync.fail" 2>/dev/null || echo 0)
            if [ "$n" -gt 0 ]; then
              echo $((n-1)) > "$d/rsync.fail"
              cat "$d/rsync.msg" 2>/dev/null || echo "connection reset"
              exit 12
            fi
            for a in "$@"; do case "$a" in -*) ;; *) if [ -z "$src" ]; then src=$a; else dst=$a; fi;; esac; done
            dst=${dst#*:}
            mkdir -p "$dst" && cp -r "$src" "$dst/"
            """);
        var verify = script("verify", """
            if [ -f "$(dirname "$0")/verify.invalid" ]; then echo "checksum failure"; exit 1; fi
            for a in "$@"; do last=$a; done
            [ -d "$last" ]
            """);
        var ssh = script("ssh", """
            for a in "$@"; do last=$a; done
            exec sh -c "$last"
            """);

        config = new DmfTarArchiveProviderFactory();
        config.setDmfTarExecutable(dmftar);
        config.setSshExecutable(ssh);
        config.setUser("vault");
        config.setHost("localhost");
        config.setArchiveRoot(remoteRoot);
        config.setLocalTransferRoot(localTransferRoot);
        config.getRsyncUpload().setExecutable(rsync.toString());
        config.getVerify().setCommand(new ExternalCommandConfig(verify.toString(), List.of("${remoteTransferDir}/${name}")));
        config.getRetry().setMaxAttempts(3);
        config.getRetry().setBackoff(Duration.milliseconds(1));
        config.getMoratorium().setDuration(Duration.milliseconds(300));
        moratoriumManager = new MoratoriumManager(config.getMoratorium());
    }

    private Path script(String name, String body) throws IOException {
        var file = binDir.resolve(name);
        Files.writeString(file, "#!/bin/sh\n" + body);
        assertThat(file.toFile().setExecutable(true)).isTrue();
        return file;
    }

    private RsyncedDmfTarArchive createArchive(boolean exists) {
        return new RsyncedDmfTarArchive(LAYER_ID, mock(Archive.class), exists, config, new ExternalCommandRunner(moratoriumManager, config.getRetry()),
            new LocalDmfTarRunner(config.getDmfTarExecutable()), moratoriumManager);
    }

    private long dmftarCalls() throws IOException {
        var calls = binDir.resolve("dmftar.calls");
        return Files.exists(calls) ? Files.readAllLines(calls).size() : 0;
    }

    @Test
    public void archiveFrom_puts_verified_dmftar_in_remote_root_and_cleans_up_locally() throws Exception {
        var archive = createArchive(false);

        archive.archiveFrom(stagingDir);

        assertThat(archive.isArchived()).isTrue();
        assertThat(remoteRoot.resolve(NAME).resolve("0000-volume")).exists();
        assertThat(remoteRoot.resolve(".incoming").resolve(NAME)).doesNotExist();
        assertThat(localTransferRoot.resolve(NAME)).doesNotExist();
        assertThat(localTransferRoot.resolve(NAME + ".complete")).doesNotExist();
        assertThat(stagingDir.resolve("data.txt")).exists();
    }

    @Test
    public void archiveFrom_retries_failed_uploads() throws Exception {
        Files.writeString(binDir.resolve("rsync.fail"), "2");
        var archive = createArchive(false);

        archive.archiveFrom(stagingDir);

        assertThat(archive.isArchived()).isTrue();
        assertThat(remoteRoot.resolve(NAME).resolve("0000-volume")).exists();
        assertThat(Files.readString(binDir.resolve("rsync.fail")).trim()).isEqualTo("0");
    }

    @Test
    public void archiveFrom_gives_up_after_max_attempts_and_keeps_local_dmftar_for_next_attempt() throws Exception {
        Files.writeString(binDir.resolve("rsync.fail"), "10");
        var archive = createArchive(false);

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(CommandFailedException.class);

        assertThat(archive.isArchived()).isFalse();
        assertThat(remoteRoot.resolve(NAME)).doesNotExist();
        assertThat(localTransferRoot.resolve(NAME)).exists();
        assertThat(localTransferRoot.resolve(NAME + ".complete")).exists();
    }

    @Test
    public void archiveFrom_starts_moratorium_and_resumes_with_existing_local_dmftar_after_it() throws Exception {
        Files.writeString(binDir.resolve("rsync.fail"), "1");
        Files.writeString(binDir.resolve("rsync.msg"), "ssh: connect to host localhost port 22: Connection refused");
        var archive = createArchive(false);

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(MoratoriumException.class);
        assertThat(moratoriumManager.isUnderMoratorium()).isTrue();
        assertThatThrownBy(() -> archive.archiveFrom(stagingDir)).isInstanceOf(MoratoriumException.class);

        Thread.sleep(400);
        archive.archiveFrom(stagingDir);

        assertThat(archive.isArchived()).isTrue();
        assertThat(remoteRoot.resolve(NAME).resolve("0000-volume")).exists();
        assertThat(dmftarCalls()).isEqualTo(1);
    }

    @Test
    public void archiveFrom_removes_invalid_upload_and_local_dmftar() throws Exception {
        Files.createFile(binDir.resolve("verify.invalid"));
        var archive = createArchive(false);

        assertThatThrownBy(() -> archive.archiveFrom(stagingDir))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("checksum failure");

        assertThat(archive.isArchived()).isFalse();
        assertThat(remoteRoot.resolve(NAME)).doesNotExist();
        assertThat(remoteRoot.resolve(".incoming").resolve(NAME)).doesNotExist();
        assertThat(localTransferRoot.resolve(NAME)).doesNotExist();
        assertThat(localTransferRoot.resolve(NAME + ".complete")).doesNotExist();
    }

    @Test
    public void archiveFrom_replaces_existing_remote_archive() throws Exception {
        Files.createDirectories(remoteRoot.resolve(NAME));
        Files.writeString(remoteRoot.resolve(NAME).resolve("old-volume"), "old");
        var archive = createArchive(true);

        archive.archiveFrom(stagingDir);

        assertThat(remoteRoot.resolve(NAME).resolve("0000-volume")).exists();
        assertThat(remoteRoot.resolve(NAME).resolve("old-volume")).doesNotExist();
        assertThat(remoteRoot.resolve(".incoming").resolve(NAME + ".bak")).doesNotExist();
    }

    @Test
    public void archiveFrom_rebuilds_incomplete_local_dmftar_and_removes_stale_remote_upload() throws Exception {
        Files.createDirectories(localTransferRoot.resolve(NAME));
        Files.writeString(localTransferRoot.resolve(NAME).resolve("junk"), "partial");
        Files.createDirectories(remoteRoot.resolve(".incoming").resolve(NAME));
        Files.writeString(remoteRoot.resolve(".incoming").resolve(NAME).resolve("stale-volume"), "stale");
        var archive = createArchive(false);

        archive.archiveFrom(stagingDir);

        assertThat(remoteRoot.resolve(NAME).resolve("0000-volume")).exists();
        assertThat(remoteRoot.resolve(NAME).resolve("junk")).doesNotExist();
        assertThat(remoteRoot.resolve(NAME).resolve("stale-volume")).doesNotExist();
    }

    @Test
    public void archiveFrom_removes_leftover_dmftar_cache_directories_from_staging_dir() throws Exception {
        Files.createDirectories(stagingDir.resolve("dmftar-cache.123"));
        var archive = createArchive(false);

        archive.archiveFrom(stagingDir);

        assertThat(stagingDir.resolve("dmftar-cache.123")).doesNotExist();
        assertThat(stagingDir.resolve("data.txt")).exists();
    }
}
