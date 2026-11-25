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
package nl.knaw.dans.datavault.resources;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import nl.knaw.dans.datavault.DdDataVaultApplication;
import nl.knaw.dans.datavault.core.LayerArchiveJob;
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerState;
import nl.knaw.dans.layerstore.LayeredItemStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.ws.rs.core.MediaType;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LayersApiResourceHttpTest {
    private static final long ID = 42L;
    private static final LayeredItemStore STORE = mock(LayeredItemStore.class);
    private static final LayerArchiveJobDao JOBS = mock(LayerArchiveJobDao.class);

    @RegisterExtension
    static final DropwizardAppExtension<Configuration> APP = new DropwizardAppExtension<>(TestApplication.class, null, ConfigOverride.randomPorts());

    public static class TestApplication extends Application<Configuration> {
        @Override
        public void run(Configuration configuration, Environment environment) {
            DdDataVaultApplication.configureObjectMapper(environment.getObjectMapper());
            environment.jersey().register(new LayersApiResource(STORE, JOBS));
        }
    }

    @Test
    public void archived_and_failed_layer_status_are_serialized_over_http() throws Exception {
        when(STORE.getLayerState(ID)).thenReturn(LayerState.ARCHIVED);
        var target = APP.client().target("http://localhost:" + APP.getLocalPort() + "/layers/" + ID);
        try (var response = target.request(MediaType.APPLICATION_JSON_TYPE).get()) {
            assertThat(response.getStatus()).isEqualTo(200);
            assertThat(response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)).isTrue();
            var json = APP.getObjectMapper().readTree(response.readEntity(String.class));
            assertThat(json.get("layerId").asLong()).isEqualTo(ID);
            assertThat(json.get("status").asText()).isEqualTo("ARCHIVED");
            assertThat(json.has("sizeInBytes")).isTrue();
            assertThat(json.get("sizeInBytes").isNull()).isTrue();
            assertThat(json.has("message")).isFalse();
        }

        var message = "failure \"quoted\"\\path\nnext line";
        when(STORE.getLayerState(ID)).thenReturn(LayerState.CLOSED);
        when(STORE.getLayerSizeInBytes(ID)).thenReturn(123L);
        when(JOBS.findLatestByLayer(ID)).thenReturn(Optional.of(LayerArchiveJob.builder().status(LayerArchiveJob.Status.FAILED).message(message).build()));
        try (var response = target.request(MediaType.APPLICATION_JSON_TYPE).get()) {
            assertThat(response.getStatus()).isEqualTo(200);
            assertThat(response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)).isTrue();
            var body = response.readEntity(String.class);
            assertThat(body).contains("failure \\\"quoted\\\"\\\\path\\nnext line");
            var json = APP.getObjectMapper().readTree(body);
            assertThat(json.get("layerId").asLong()).isEqualTo(ID);
            assertThat(json.get("status").asText()).isEqualTo("CLOSED");
            assertThat(json.get("sizeInBytes").asLong()).isEqualTo(123L);
            assertThat(json.get("message").asText()).isEqualTo(message);
        }
    }
}