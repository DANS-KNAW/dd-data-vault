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
import nl.knaw.dans.datavault.Conversions;
import nl.knaw.dans.datavault.api.ImportCommandDto;
import nl.knaw.dans.datavault.core.ImportService;
import nl.knaw.dans.datavault.core.InvalidImportException;
import org.mapstruct.factory.Mappers;

import javax.ws.rs.core.Response;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.OK;

@AllArgsConstructor
public class ImportsApiResource implements ImportsApi {
    private final Conversions conversions = Mappers.getMapper(Conversions.class);
    private final ImportService importService;

    @Override
    public Response importsGet() {
        return null;
    }

    @Override
    public Response importsIdGet(UUID id) {
        return Response
            .status(OK)
            .entity(conversions.convert(importService.getImport(id)))
            .build();
    }

    @Override
    public Response importsPost(ImportCommandDto importJobDto) {
        try {
            return Response
                .status(CREATED)
                .entity(conversions.convert(importService.addImport(importJobDto)))
                .build();
        }
        catch (InvalidImportException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }
}
