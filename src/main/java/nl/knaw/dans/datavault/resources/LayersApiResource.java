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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.AllArgsConstructor;
import nl.knaw.dans.datavault.api.LayerStatusDto;
import nl.knaw.dans.datavault.core.LayerArchiveJob;
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerState;
import nl.knaw.dans.layerstore.LayeredItemStore;

import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@AllArgsConstructor
public class LayersApiResource implements LayersApi {
    private final LayeredItemStore layeredItemStore;
    private final LayerArchiveJobDao archiveJobDao;

    /*
     * The @UnitOfWork annotation will do no good here, as the archiving process is off-loaded to a separate thread. The part that interacts with the database is the LayerConsistencyChecker which
     * is wrapped in a UnitOfWorkAwareProxy on app initialization.
     */
    @Override
    public Response layersIdArchivePost(Long layerId) {
        try {
            layeredItemStore.archiveLayer(layerId, false);
            return Response.status(ACCEPTED).build();
        }
        catch (IllegalArgumentException e) {
            return Response.status(NOT_FOUND).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
    }

    @Override
    public Response layersIdClosePost(Long layerId) {
        try {
            layeredItemStore.closeLayer(layerId);
            return Response.status(OK).build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @UnitOfWork
    public Response layersIdGet(Long layerId) {
        try {
            return Response.ok(layerStatus(layerId)).build();
        }
        catch (IllegalArgumentException e) {
            return Response.status(NOT_FOUND).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response layersIdRearchivePost(Long layerId) {
        try {
            layeredItemStore.archiveLayer(layerId, true);
            return Response.status(ACCEPTED).build();
        }
        catch (IllegalArgumentException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @Override
    public Response layersIdReopenPost(Long layerId) {
        return null;
    }

    @Override
    @UnitOfWork
    public Response layersIdsGet() {
        try {
            return Response.ok(layeredItemStore.listLayerIds())
                .build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    @UnitOfWork
    public Response layersPost() {
        try {
            return Response.status(CREATED).entity(layerStatus(layeredItemStore.newTopLayer())).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    @UnitOfWork
    public Response layersTopGet() {
        try {
            return Response.ok(layerStatus(layeredItemStore.getTopLayerId())).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    private LayerStatusDto layerStatus(long layerId) throws IOException {
        var state = layeredItemStore.getLayerState(layerId);
        Long size = null;
        if (state != LayerState.ARCHIVED) {
            try {
                size = layeredItemStore.getLayerSizeInBytes(layerId);
            }
            catch (IOException | RuntimeException e) {
                if (layeredItemStore.getLayerState(layerId) != LayerState.ARCHIVED) {
                    throw e;
                }
            }
            state = layeredItemStore.getLayerState(layerId);
            if (state == LayerState.ARCHIVED) {
                size = null;
            }
        }
        var result = new LayerStatusDto().layerId(layerId)
            .status(LayerStatusDto.StatusEnum.valueOf(state.name())).sizeInBytes(size);
        if (state != LayerState.ARCHIVED) {
            archiveJobDao.findLatestByLayer(layerId)
                .filter(job -> job.getStatus() == LayerArchiveJob.Status.FAILED)
                .ifPresent(job -> result.message(job.getMessage()));
        }
        return result;
    }
}
