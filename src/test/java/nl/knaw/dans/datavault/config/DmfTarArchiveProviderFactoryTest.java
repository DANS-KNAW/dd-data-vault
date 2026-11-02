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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;
import nl.knaw.dans.layerstore.DmfTarArchiveProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DmfTarArchiveProviderFactoryTest {

    @Test
    public void dmftar_provider_can_be_parsed_and_built_with_defaults() throws Exception {
        var yaml = """
            type: DMFTAR
            dmfTarExecutable: /usr/local/bin/dmftar
            sshExecutable: /usr/bin/ssh
            host: archive.example.org
            user: vault
            archiveRoot: /archive
            localPackagingDir: /data/vault/packaging
            """;
        var mapper = Jackson.newObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        var factory = mapper.readValue(yaml, ArchiveProviderFactory.class);

        assertThat(factory.build()).isInstanceOf(DmfTarArchiveProvider.class);
    }

    @Test
    public void dmftar_provider_can_be_parsed_with_all_settings() throws Exception {
        var yaml = """
            type: DMFTAR
            dmfTarExecutable: /usr/local/bin/dmftar
            sshExecutable: /usr/bin/ssh
            rsyncExecutable: /usr/bin/rsync
            rsyncArgs: [ "-av", "--partial", "--append" ]
            host: archive.example.org
            user: vault
            archiveRoot: /archive
            localPackagingDir: /data/vault/packaging
            maxRetries: 3
            retryInterval: 30 seconds
            verify: false
            invalidOn: "checksum failure"
            moratorium:
              triggerOn: "Connection refused"
              duration: 15 minutes
            """;
        var mapper = Jackson.newObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

        assertThat(mapper.readValue(yaml, ArchiveProviderFactory.class).build()).isInstanceOf(DmfTarArchiveProvider.class);
    }
}
