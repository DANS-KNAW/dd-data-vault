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

import nl.knaw.dans.datavault.api.CopyDirectoryIntoRequestDto;
import nl.knaw.dans.datavault.api.CopyFileToRequestDto;
import nl.knaw.dans.datavault.config.ItemstoreConfig;
import nl.knaw.dans.datavault.config.ItemstoreEndpointsConfig;
import nl.knaw.dans.layerstore.ItemStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ItemstoreApiResourceTest {
    private ItemStore layeredItemStore;
    private ItemstoreConfig itemstoreConfig;
    private ItemstoreApiResource resource;

    @BeforeEach
    public void setUp() {
        layeredItemStore = Mockito.mock(ItemStore.class);
        itemstoreConfig = new ItemstoreConfig();
        var endpoints = new ItemstoreEndpointsConfig();
        endpoints.setCopyDirectoryInto(true);
        endpoints.setCopyFileTo(true);
        itemstoreConfig.setEnableEndpoints(endpoints);
        itemstoreConfig.setWorkDir("target/test/ItemstoreApiResourceTest/work");
        resource = new ItemstoreApiResource(layeredItemStore, itemstoreConfig);
    }

    @Test
    public void itemstoreCopyDirectoryIntoPost_should_return_bad_request_when_source_is_not_absolute() {
        var request = new CopyDirectoryIntoRequestDto();
        request.setSource("relative/path");
        request.setDestination("dest");

        var response = resource.itemstoreCopyDirectoryIntoPost(request);

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals("Source path must be absolute", response.getEntity());
    }

    @Test
    public void itemstoreCopyFileToPost_should_return_bad_request_when_source_is_not_absolute() {
        var request = new CopyFileToRequestDto();
        request.setSource("relative/path.txt");
        request.setDestination("dest.txt");

        var response = resource.itemstoreCopyFileToPost(request);

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals("Source path must be absolute", response.getEntity());
    }
}
