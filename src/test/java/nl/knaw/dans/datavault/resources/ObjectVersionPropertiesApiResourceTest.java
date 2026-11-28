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

import com.fasterxml.jackson.databind.node.TextNode;
import nl.knaw.dans.datavault.core.RepositoryProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ObjectVersionPropertiesApiResourceTest {
    private RepositoryProvider repositoryProvider;
    private ObjectVersionPropertiesApiResource resource;

    @BeforeEach
    public void setUp() {
        repositoryProvider = mock(RepositoryProvider.class);
        resource = new ObjectVersionPropertiesApiResource(repositoryProvider);
    }

    @Test
    public void get_all_passes_null_version_for_latest() {
        when(repositoryProvider.getObjectVersionProperties("id", null)).thenReturn(Map.of("a", "b"));

        var response = resource.objectVersionPropertiesIdNrGet("id", "latest");

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getEntity()).isEqualTo(Map.of("a", "b"));
    }

    @Test
    public void get_one_returns_string_value_as_json_node() {
        when(repositoryProvider.getObjectVersionProperties("id", "v1")).thenReturn(Map.of("dataset-version", "1.0"));

        var response = resource.objectVersionPropertiesIdNrNameGet("id", "1", "dataset-version");

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getEntity()).isEqualTo(new TextNode("1.0"));
    }

    @Test
    public void get_one_returns_404_for_absent_property() {
        when(repositoryProvider.getObjectVersionProperties("id", "v1")).thenReturn(Map.of());

        assertThat(resource.objectVersionPropertiesIdNrNameGet("id", "v1", "deaccessioned").getStatus()).isEqualTo(404);
    }

    @Test
    public void get_returns_404_for_unknown_object() {
        when(repositoryProvider.getObjectVersionProperties("missing", "v1")).thenThrow(new NoSuchElementException("not found"));

        assertThat(resource.objectVersionPropertiesIdNrGet("missing", "1").getStatus()).isEqualTo(404);
    }

    @Test
    public void put_accepts_version_with_and_without_v_prefix() {
        var value = Map.of("reason", "Invalid dataset");

        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "1", "deaccessioned", value).getStatus()).isEqualTo(204);
        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "v2", "deaccessioned", value).getStatus()).isEqualTo(204);

        verify(repositoryProvider).setObjectVersionProperty("id", "v1", "deaccessioned", value);
        verify(repositoryProvider).setObjectVersionProperty("id", "v2", "deaccessioned", value);
    }

    @Test
    public void put_rejects_latest_invalid_version_and_null_value() {
        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "latest", "p", "x").getStatus()).isEqualTo(400);
        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "one", "p", "x").getStatus()).isEqualTo(400);
        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "1", "p", null).getStatus()).isEqualTo(400);
        verify(repositoryProvider, never()).setObjectVersionProperty(anyString(), anyString(), anyString(), any());
    }

    @Test
    public void put_maps_validation_error_to_400_with_message() {
        doThrow(new IllegalArgumentException("Unknown property per property-registry: p"))
            .when(repositoryProvider).setObjectVersionProperty("id", "v1", "p", "x");

        var response = resource.objectVersionPropertiesIdNrNamePut("id", "1", "p", "x");

        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getEntity()).isEqualTo("Unknown property per property-registry: p");
    }

    @Test
    public void put_maps_blocked_object_to_409() {
        doThrow(new IllegalStateException("Object is blocked: id"))
            .when(repositoryProvider).setObjectVersionProperty("id", "v1", "p", "x");

        assertThat(resource.objectVersionPropertiesIdNrNamePut("id", "1", "p", "x").getStatus()).isEqualTo(409);
    }

    @Test
    public void delete_returns_204_and_maps_absent_property_to_404() {
        doThrow(new NoSuchElementException("not found"))
            .when(repositoryProvider).deleteObjectVersionProperty("id", "v1", "absent");

        assertThat(resource.objectVersionPropertiesIdNrNameDelete("id", "1", "deaccessioned").getStatus()).isEqualTo(204);
        assertThat(resource.objectVersionPropertiesIdNrNameDelete("id", "1", "absent").getStatus()).isEqualTo(404);
        verify(repositoryProvider).deleteObjectVersionProperty("id", "v1", "deaccessioned");
    }
}
