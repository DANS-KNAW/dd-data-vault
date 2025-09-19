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
import nl.knaw.dans.datavault.Conversions;
import nl.knaw.dans.datavault.api.ImportCommandDto;
import nl.knaw.dans.datavault.core.ImportBatch;
import nl.knaw.dans.datavault.db.ImportBatchDao;
import org.mapstruct.factory.Mappers;

import javax.ws.rs.core.Response;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.CREATED;

@AllArgsConstructor
public class ImportsApiResource implements ImportsApi {
    private final Conversions conversions = Mappers.getMapper(Conversions.class);
    private final ImportBatchDao importBatchDao;
    private final Path inbox;

    @Override
    @UnitOfWork
    public Response importsGet() {
        return null;
    }

    @Override
    @UnitOfWork
    public Response importsIdGet(UUID id) {
        var batch = importBatchDao.get(id);
        if (batch == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(conversions.convert(batch)).build();
    }

    @Override
    @UnitOfWork
    public Response importsPost(ImportCommandDto importJobDto) {
        try {
            var importBatch = conversions.convert(importJobDto);
            importBatch.setPath(getInboxRelativePath(Path.of(importBatch.getPath())));
            importBatch.setCreated(OffsetDateTime.now());
            importBatch.setStatus(ImportBatch.Status.PENDING);
            return Response
                .status(CREATED)
                .entity(conversions.convert(importBatchDao.create(importBatch)))
                .build();
        }
        catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    private String getInboxRelativePath(Path path) {
        Path normalizedPath = path.toAbsolutePath().normalize();
        Path normalizedInbox = inbox.toAbsolutePath().normalize();
        var relativePath = normalizedInbox.relativize(normalizedPath).toString();

        if (normalizedPath.startsWith(normalizedInbox)) {
            return relativePath;
        }
        throw new IllegalArgumentException("Path traverses outside of inbox directory");
    }
}
