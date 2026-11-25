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

import io.dropwizard.jackson.Jackson;
import nl.knaw.dans.datavault.LayerStatusJsonMixin;
import nl.knaw.dans.datavault.api.LayerStatusDto;
import nl.knaw.dans.datavault.core.LayerArchiveJob;
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerState;
import nl.knaw.dans.layerstore.LayeredItemStore;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LayersApiResourceTest {
    private static final long ID = 42L;
    private final LayeredItemStore store = mock(LayeredItemStore.class);
    private final LayerArchiveJobDao jobs = mock(LayerArchiveJobDao.class);
    private final LayersApiResource resource = new LayersApiResource(store, jobs);

    @Test
    public void id_returns_open_and_closed_status_with_size() throws Exception {
        for (var state : new LayerState[] { LayerState.OPEN, LayerState.CLOSED }) {
            when(store.getLayerState(ID)).thenReturn(state);
            when(store.getLayerSizeInBytes(ID)).thenReturn(123L);
            when(jobs.findLatestByLayer(ID)).thenReturn(Optional.empty());

            var response = resource.layersIdGet(ID);
            assertStatus(response, 200, state.name(), 123L, null);
        }
    }

    @Test
    public void top_and_create_return_status_for_the_returned_layer_id() throws Exception {
        when(store.getTopLayerId()).thenReturn(ID);
        when(store.newTopLayer()).thenReturn(ID);
        when(store.getLayerState(ID)).thenReturn(LayerState.OPEN);
        when(store.getLayerSizeInBytes(ID)).thenReturn(12L);
        when(jobs.findLatestByLayer(ID)).thenReturn(Optional.of(job(LayerArchiveJob.Status.FAILED, "archive failed")));

        assertStatus(resource.layersTopGet(), 200, "OPEN", 12L, "archive failed");
        assertStatus(resource.layersPost(), 201, "OPEN", 12L, "archive failed");
        verify(store).getTopLayerId();
        verify(store).newTopLayer();
    }

    @Test
    public void latest_failed_job_message_is_returned_but_other_outcomes_are_suppressed() throws Exception {
        when(store.getLayerState(ID)).thenReturn(LayerState.CLOSED);
        when(store.getLayerSizeInBytes(ID)).thenReturn(99L);
        when(jobs.findLatestByLayer(ID)).thenReturn(Optional.of(job(LayerArchiveJob.Status.FAILED, "failure")));
        assertStatus(resource.layersIdGet(ID), 200, "CLOSED", 99L, "failure");

        for (var status : new LayerArchiveJob.Status[] { LayerArchiveJob.Status.PENDING, LayerArchiveJob.Status.RUNNING, LayerArchiveJob.Status.SUCCESS }) {
            when(jobs.findLatestByLayer(ID)).thenReturn(Optional.of(job(status, "old failure")));
            assertStatus(resource.layersIdGet(ID), 200, "CLOSED", 99L, null);
        }
        when(jobs.findLatestByLayer(ID)).thenReturn(Optional.empty());
        assertStatus(resource.layersIdGet(ID), 200, "CLOSED", 99L, null);
    }

    @Test
    public void archived_layer_has_no_size_or_failure_message_and_does_not_read_them() throws Exception {
        when(store.getLayerState(ID)).thenReturn(LayerState.ARCHIVED);

        assertStatus(resource.layersIdGet(ID), 200, "ARCHIVED", null, null);
        verify(store, never()).getLayerSizeInBytes(ID);
        verify(jobs, never()).findLatestByLayer(ID);
    }

    @Test
    public void archive_transition_discards_successful_size() throws Exception {
        when(store.getLayerState(ID)).thenReturn(LayerState.CLOSED, LayerState.ARCHIVED);
        when(store.getLayerSizeInBytes(ID)).thenReturn(99L);

        assertStatus(resource.layersIdGet(ID), 200, "ARCHIVED", null, null);
        verify(jobs, never()).findLatestByLayer(ID);
    }

    @Test
    public void archive_transition_suppresses_size_errors() throws Exception {
        for (var exception : new Exception[] { new IOException("size"), new IllegalStateException("size") }) {
            when(store.getLayerState(ID)).thenReturn(LayerState.CLOSED, LayerState.ARCHIVED, LayerState.ARCHIVED);
            doThrow(exception).when(store).getLayerSizeInBytes(ID);
            assertStatus(resource.layersIdGet(ID), 200, "ARCHIVED", null, null);
        }
        verify(jobs, never()).findLatestByLayer(ID);
    }

    @Test
    public void missing_layer_and_size_failures_keep_their_error_outcomes() throws Exception {
        when(store.getLayerState(ID)).thenThrow(new IllegalArgumentException("missing"));
        assertThat(resource.layersIdGet(ID).getStatus()).isEqualTo(404);

        doReturn(LayerState.CLOSED).when(store).getLayerState(ID);
        when(store.getLayerSizeInBytes(ID)).thenThrow(new IOException("size"));
        assertThat(resource.layersIdGet(ID).getStatus()).isEqualTo(500);

        doThrow(new IllegalStateException("size")).when(store).getLayerSizeInBytes(ID);
        assertThatThrownBy(() -> resource.layersIdGet(ID)).isInstanceOf(IllegalStateException.class).hasMessage("size");
        verify(jobs, never()).findLatestByLayer(ID);
    }

    @Test
    public void top_and_create_report_size_io_failure() throws Exception {
        when(store.getTopLayerId()).thenReturn(ID);
        when(store.newTopLayer()).thenReturn(ID);
        when(store.getLayerState(ID)).thenReturn(LayerState.OPEN);
        when(store.getLayerSizeInBytes(ID)).thenThrow(new IOException("size"));
        assertThat(resource.layersTopGet().getStatus()).isEqualTo(500);
        assertThat(resource.layersPost().getStatus()).isEqualTo(500);
    }

    @Test
    public void mixin_serializes_null_size_and_omits_null_message() throws Exception {
        var mapper = Jackson.newObjectMapper();
        mapper.addMixIn(LayerStatusDto.class, LayerStatusJsonMixin.class);
        when(store.getLayerState(ID)).thenReturn(LayerState.ARCHIVED);
        var archived = mapper.readTree(mapper.writeValueAsString(resource.layersIdGet(ID).getEntity()));
        assertThat(archived.get("layerId").asLong()).isEqualTo(ID);
        assertThat(archived.get("status").asText()).isEqualTo("ARCHIVED");
        assertThat(archived.has("sizeInBytes")).isTrue();
        assertThat(archived.get("sizeInBytes").isNull()).isTrue();
        assertThat(archived.has("message")).isFalse();

        when(store.getLayerState(ID)).thenReturn(LayerState.CLOSED);
        when(store.getLayerSizeInBytes(ID)).thenReturn(123L);
        when(jobs.findLatestByLayer(ID)).thenReturn(Optional.of(job(LayerArchiveJob.Status.FAILED, "failure")));
        var failed = mapper.readTree(mapper.writeValueAsString(resource.layersIdGet(ID).getEntity()));
        assertThat(failed.get("sizeInBytes").asLong()).isEqualTo(123L);
        assertThat(failed.get("message").asText()).isEqualTo("failure");
    }

    private LayerArchiveJob job(LayerArchiveJob.Status status, String message) {
        return LayerArchiveJob.builder().status(status).message(message).build();
    }

    private void assertStatus(Response response, int code, String state, Long size, String message) {
        assertThat(response.getStatus()).isEqualTo(code);
        assertThat(response.getEntity()).isInstanceOf(LayerStatusDto.class);
        var dto = (LayerStatusDto) response.getEntity();
        assertThat(dto.getLayerId()).isEqualTo(ID);
        assertThat(dto.getStatus().name()).isEqualTo(state);
        assertThat(dto.getSizeInBytes()).isEqualTo(size);
        assertThat(dto.getMessage()).isEqualTo(message);
    }
}