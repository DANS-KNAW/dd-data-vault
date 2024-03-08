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

import lombok.AllArgsConstructor;
import nl.knaw.dans.datavault.api.LayerStatusDto;
import nl.knaw.dans.layerstore.LayerManager;

import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.Response.Status.CREATED;

@AllArgsConstructor
public class LayersApiResource implements LayersApi {
    private final LayerManager layerManager;

    @Override
    public Response layersLayerIdGet(Long layerId) {
        try {
            var layer = layerManager.getLayer(layerId);
            return Response.ok(new LayerStatusDto()
                    .layerId(layer.getId())
                    .sizeInBytes(layer.getSizeInBytes()))
                .build();
        }
        catch (IllegalArgumentException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        catch (IOException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response layersPost() {
        layerManager.newTopLayer();
        return Response.status(CREATED).entity(new LayerStatusDto().layerId(layerManager.getTopLayer().getId())).build();
        // TODO: error handling
    }

    @Override
    public Response layersTopGet() {
        var topLayer = layerManager.getTopLayer();
        try {
            return Response.ok(new LayerStatusDto()
                    .layerId(topLayer.getId())
                    .sizeInBytes(topLayer.getSizeInBytes()))
                .build();
        }
        catch (IOException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }
}
