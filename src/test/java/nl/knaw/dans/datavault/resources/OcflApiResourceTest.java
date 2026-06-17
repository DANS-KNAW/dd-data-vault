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

import nl.knaw.dans.datavault.core.RepositoryProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

public class OcflApiResourceTest {
    private RepositoryProvider repositoryProvider;
    private OcflApiResource resource;

    @BeforeEach
    public void setUp() {
        repositoryProvider = Mockito.mock(RepositoryProvider.class);
        resource = new OcflApiResource(repositoryProvider);
    }

    @Test
    public void ocflObjectsIdVersionsNrDetailsGet_should_pass_null_when_nr_is_latest() {
        resource.ocflObjectsIdVersionsNrDetailsGet("id", "latest");
        verify(repositoryProvider).getVersionDetails(eq("id"), isNull());
    }

    @Test
    public void ocflObjectsIdVersionsNrDetailsGet_should_pass_v_prefixed_number_when_nr_is_integer() {
        resource.ocflObjectsIdVersionsNrDetailsGet("id", "1");
        verify(repositoryProvider).getVersionDetails(eq("id"), eq("v1"));
    }

    @Test
    public void ocflObjectsIdVersionsNrDetailsGet_should_pass_v_prefixed_number_when_nr_is_v_prefixed_integer() {
        resource.ocflObjectsIdVersionsNrDetailsGet("id", "v1");
        verify(repositoryProvider).getVersionDetails(eq("id"), eq("v1"));
    }

    @Test
    public void ocflObjectsIdVersionsNrDetailsGet_should_return_bad_request_when_nr_is_invalid() {
        var response = resource.ocflObjectsIdVersionsNrDetailsGet("id", "invalid");
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

    @Test
    public void ocflObjectsIdVersionsNrFilesGet_should_pass_null_when_nr_is_latest() {
        resource.ocflObjectsIdVersionsNrFilesGet("id", "latest");
        verify(repositoryProvider).listFiles(eq("id"), isNull());
    }

    @Test
    public void ocflObjectsIdVersionsNrFilesGet_should_pass_v_prefixed_number_when_nr_is_integer() {
        resource.ocflObjectsIdVersionsNrFilesGet("id", "1");
        verify(repositoryProvider).listFiles(eq("id"), eq("v1"));
    }

    @Test
    public void ocflObjectsIdVersionsNrFilesGet_should_return_bad_request_when_nr_is_invalid() {
        var response = resource.ocflObjectsIdVersionsNrFilesGet("id", "invalid");
        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    }

}
