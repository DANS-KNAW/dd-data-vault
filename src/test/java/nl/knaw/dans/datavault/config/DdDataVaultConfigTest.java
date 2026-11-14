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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Duration;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.validation.Validation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DdDataVaultConfigTest {
    private static final Path testDir = Path.of("target/test/DdDataVaultConfigTest");

    @BeforeAll
    public static void setUp() throws IOException {
        FileUtils.deleteQuietly(testDir.toFile());
        Files.createDirectories(testDir);
    }

    private DdDataVaultConfig parse(Path configFile) throws Exception {
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var factory = new YamlConfigurationFactory<>(DdDataVaultConfig.class, validatorFactory.getValidator(),
                Jackson.newObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true), "dw");
            return factory.build(configFile.toFile());
        }
    }

    private Path debugConfigWithArchiveProvider(String name, String archiveProviderYaml) throws IOException {
        var debugConfig = Files.readString(Path.of("src/test/resources/debug-etc/config.yml"));
        var tarBlock = "    archiveProvider:\n      type: TAR\n      archiveRoot: data/vault/archive\n";
        assertThat(debugConfig).contains(tarBlock);
        var file = testDir.resolve(name);
        Files.writeString(file, debugConfig.replace(tarBlock, archiveProviderYaml.indent(4)));
        return file;
    }

    @Test
    public void default_configuration_can_be_parsed() throws Exception {
        assertThat(parse(Path.of("src/main/assembly/dist/cfg/config.yml"))).isNotNull();
    }

    @Test
    public void debug_etc_configuration_can_be_parsed() throws Exception {
        var config = parse(Path.of("src/test/resources/debug-etc/config.yml"));
        assertThat(config.getDataVault().getLayerStore().getLayerArchiver().getMaxParallelArchiveJobs()).isEqualTo(2);
    }

    @Test
    public void dmftar_configuration_with_only_required_settings_gets_defaults() throws Exception {
        var file = debugConfigWithArchiveProvider("minimal.yml", """
            archiveProvider:
              type: DMFTAR
              dmfTarExecutable: /usr/local/bin/dmftar
              sshExecutable: /usr/bin/ssh
              host: dmf.example.org
              user: vault
              archiveRoot: /remote/archive
              localTransferRoot: data/vault/transfer
            """);

        var factory = (DmfTarArchiveProviderFactory) parse(file).getDataVault().getLayerStore().getArchiveProvider();

        assertThat(factory.getArchiveRoot()).isEqualTo(Path.of("/remote/archive"));
        assertThat(factory.getLocalTransferRoot()).isEqualTo(Path.of("data/vault/transfer"));
        assertThat(factory.getRsyncUpload().getExecutable()).isEqualTo("/usr/bin/rsync");
        assertThat(factory.getRsyncUpload().getArgs()).contains("--partial", "${localDir}/${name}");
        assertThat(factory.getRsyncDownload().getArgs()).contains("${localDir}/");
        assertThat(factory.getVerify().getInvalidOn()).isEqualTo("checksum failure");
        assertThat(factory.getRetry().getMaxAttempts()).isEqualTo(10);
        assertThat(factory.isVerifyDownloadedCopy()).isFalse();
        assertThat(factory.getMaxParallelUnarchive()).isEqualTo(2);
        assertThat(factory.getMoratorium().getTriggerOn()).isEqualTo("Connection refused");
        assertThat(factory.getMoratorium().getDuration()).isEqualTo(Duration.minutes(15));
    }

    @Test
    public void dmftar_configuration_with_all_settings_can_be_parsed() throws Exception {
        var file = debugConfigWithArchiveProvider("full.yml", """
            archiveProvider:
              type: DMFTAR
              dmfTarExecutable: /usr/local/bin/dmftar
              sshExecutable: /usr/bin/ssh
              host: dmf.example.org
              user: vault
              archiveRoot: /remote/archive
              localTransferRoot: data/vault/transfer
              rsyncUpload:
                executable: /bin/rsync
                args: [ "-a", "${localDir}/${name}", "${user}@${host}:${remoteDir}/" ]
              rsyncDownload:
                executable: /bin/rsync
                args: [ "-a", "${user}@${host}:${remoteDir}/${name}", "${localDir}/" ]
              verify:
                command:
                  executable: /bin/ssh
                  args: [ "${user}@${host}", "dmftar", "--verify", "-f", "${remoteDir}/${name}" ]
                invalidOn: "bad"
              verifyDownloadedCopy: true
              maxParallelUnarchive: 4
              retry:
                maxAttempts: 3
                backoff: 5 seconds
              moratorium:
                triggerOn: "refused"
                duration: 1 hour
            """);

        var factory = (DmfTarArchiveProviderFactory) parse(file).getDataVault().getLayerStore().getArchiveProvider();

        assertThat(factory.getRsyncUpload().getExecutable()).isEqualTo("/bin/rsync");
        assertThat(factory.isVerifyDownloadedCopy()).isTrue();
        assertThat(factory.getMaxParallelUnarchive()).isEqualTo(4);
        assertThat(factory.getVerify().getInvalidOn()).isEqualTo("bad");
        assertThat(factory.getRetry().getBackoff()).isEqualTo(Duration.seconds(5));
        assertThat(factory.getMoratorium().getDuration()).isEqualTo(Duration.hours(1));
    }

    @Test
    public void dmftar_configuration_without_local_transfer_root_is_rejected() throws Exception {
        var file = debugConfigWithArchiveProvider("no-transfer-root.yml", """
            archiveProvider:
              type: DMFTAR
              dmfTarExecutable: /usr/local/bin/dmftar
              sshExecutable: /usr/bin/ssh
              host: dmf.example.org
              user: vault
              archiveRoot: /remote/archive
            """);

        assertThatThrownBy(() -> parse(file))
            .isInstanceOf(ConfigurationValidationException.class)
            .hasMessageContaining("localTransferRoot");
    }

    @Test
    public void dmftar_configuration_with_local_transfer_root_inside_staging_root_is_rejected() throws Exception {
        var file = debugConfigWithArchiveProvider("transfer-root-in-staging.yml", """
            archiveProvider:
              type: DMFTAR
              dmfTarExecutable: /usr/local/bin/dmftar
              sshExecutable: /usr/bin/ssh
              host: dmf.example.org
              user: vault
              archiveRoot: /remote/archive
              localTransferRoot: data/vault/staging/transfer
            """);

        assertThatThrownBy(() -> parse(file))
            .isInstanceOf(ConfigurationValidationException.class)
            .hasMessageContaining("localTransferRoot");
    }
}
