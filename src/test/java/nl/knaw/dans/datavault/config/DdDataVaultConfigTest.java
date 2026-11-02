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
import io.dropwizard.configuration.YamlConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.jupiter.api.Test;

import javax.validation.Validation;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class DdDataVaultConfigTest {

    private DdDataVaultConfig parseConfigurationFile(String configFile) throws Exception {
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var factory = new YamlConfigurationFactory<>(DdDataVaultConfig.class,
                validatorFactory.getValidator(),
                Jackson.newObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true), "dw");
            return factory.build(Path.of(configFile).toFile());
        }
    }

    @Test
    public void default_configuration_can_be_parsed() throws Exception {
        assertThat(parseConfigurationFile("src/main/assembly/dist/cfg/config.yml")).isNotNull();
    }

    @Test
    public void debug_etc_configuration_can_be_parsed() throws Exception {
        assertThat(parseConfigurationFile("src/test/resources/debug-etc/config.yml")).isNotNull();
    }
}
