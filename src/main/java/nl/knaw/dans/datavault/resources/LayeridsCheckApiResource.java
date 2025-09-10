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
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.datavault.core.LayerIdsCheckManager;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.UUID;

@RequiredArgsConstructor
public class LayeridsCheckApiResource implements ChecksApi {
    private final LayerIdsCheckManager layerIdsCheckManager;

    @Context
    private UriInfo uriInfo;

    @Override
    @UnitOfWork
    public Response layeridsCheckIdGet(UUID id) {
        var optionalStatus = layerIdsCheckManager.getStatus(id);
        if (optionalStatus.isPresent()) {
            return Response.ok(optionalStatus.get()).build();
        }
        else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @Override
    @UnitOfWork
    public Response layeridsCheckPost() {
        var optionalUUID = layerIdsCheckManager.trySchedule();
        if (optionalUUID.isPresent()) {
            UUID uuid = optionalUUID.get();
            URI location = uriInfo.getAbsolutePathBuilder().path(uuid.toString()).build();
            return Response.created(location).build();
        }
        else {
            return Response.status(Response.Status.CONFLICT).build();
        }
    }
}
