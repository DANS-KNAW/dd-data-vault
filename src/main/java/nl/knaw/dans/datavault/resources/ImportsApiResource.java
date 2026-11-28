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
import nl.knaw.dans.datavault.api.RestageFailedRequestDto;
import nl.knaw.dans.datavault.core.BatchCleaner;
import nl.knaw.dans.datavault.core.ImportJob;
import nl.knaw.dans.datavault.db.ImportJobDao;
import org.mapstruct.factory.Mappers;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.CREATED;

@AllArgsConstructor
public class ImportsApiResource implements ImportsApi {
    private final Conversions conversions = Mappers.getMapper(Conversions.class);
    private final ImportJobDao importJobDao;
    private final Path inbox;
    private final Path outbox;

    @Override
    @UnitOfWork
    public Response importsGet(List<String> status, Integer limit, Integer offset, String batch) {
        try {
            if ((limit != null && limit <= 0) || (offset != null && (offset < 0 || limit == null))) {
                throw new IllegalArgumentException("Invalid pagination parameters");
            }
            var statuses = EnumSet.noneOf(ImportJob.Status.class);
            if (status != null) {
                for (var value : status) {
                    statuses.add(ImportJob.Status.valueOf(value));
                }
            }
            String relativeBatch = null;
            if (batch != null) {
                var normalizedInbox = inbox.toAbsolutePath().normalize();
                var normalizedPath = normalizedInbox.resolve(Path.of(batch)).normalize();
                if (!normalizedPath.startsWith(normalizedInbox)) {
                    throw new IllegalArgumentException("Path traverses outside of inbox directory");
                }
                relativeBatch = normalizedInbox.relativize(normalizedPath).toString();
            }
            var jobs = importJobDao.listFiltered(statuses, relativeBatch, limit, offset);
            return Response.ok(conversions.convert(jobs)).build();
        }
        catch (IllegalArgumentException e) {
            return textResponse(Response.Status.BAD_REQUEST, e.getMessage());
        }
    }

    @Override
    @UnitOfWork
    public Response importsIdGet(UUID id) {
        var job = importJobDao.get(id);
        if (job == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(conversions.convert(job)).build();
    }

    @Override
    @UnitOfWork
    public Response importsPost(ImportCommandDto importJobDto) {
        try {
            var job = conversions.convert(importJobDto);
            job.setPath(getInboxRelativePath(Path.of(job.getPath())));
            job.setCreated(OffsetDateTime.now());
            job.setStatus(ImportJob.Status.PENDING);
            return Response
                .status(CREATED)
                .entity(conversions.convert(importJobDao.create(job)))
                .build();
        }
        catch (IllegalArgumentException e) {
            return textResponse(Response.Status.BAD_REQUEST, e.getMessage());
        }
    }

    @Override
    @UnitOfWork
    public Response importsCleanupPost() {
        var successfulJobs = importJobDao.findByStatus(ImportJob.Status.SUCCESS);
        for (var job : successfulJobs) {
            var cleaner = new BatchCleaner(inbox.resolve(job.getPath()), outbox.resolve(job.getPath()));
            cleaner.cleanProcessedObjects();
            cleaner.deleteBatchDirs();
        }
        return Response.ok().build();
    }

    @Override
    @UnitOfWork
    public Response importsRestageFailedPost(RestageFailedRequestDto request) {
        try {
            if (request == null || request.getSelector() == null || request.getSelector().isBlank()) {
                throw new IllegalArgumentException("Selector is required");
            }
            var selector = request.getSelector();
            UUID id = null;
            try {
                var parsed = UUID.fromString(selector);
                if (parsed.toString().equalsIgnoreCase(selector)) {
                    id = parsed;
                }
            }
            catch (IllegalArgumentException ignored) {
                // A non-UUID selector is a path.
            }
            ImportJob selected = id == null ? null : importJobDao.get(id);
            if (id != null && selected == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            var root = inbox.toAbsolutePath().normalize();
            var path = root.resolve(selected == null ? Path.of(selector) : Path.of(selected.getPath())).normalize();
            if (!path.startsWith(root) || path.equals(root)) {
                throw new IllegalArgumentException("Path must be inside inbox");
            }
            var relative = root.relativize(path);
            for (var job : importJobDao.listFiltered(EnumSet.of(ImportJob.Status.PENDING, ImportJob.Status.RUNNING), null, null, null)) {
                var active = root.resolve(job.getPath()).normalize();
                if (path.startsWith(active) || active.startsWith(path)) {
                    return textResponse(Response.Status.CONFLICT, "Import job is active");
                }
            }
            var failed = outbox.toAbsolutePath().normalize().resolve(relative).resolve("failed");
            checkNoSymlinks(root, path);
            checkNoSymlinks(outbox.toAbsolutePath().normalize(), failed);
            if (!Files.exists(failed, LinkOption.NOFOLLOW_LINKS)) {
                return Response.ok().build();
            }
            var sources = new ArrayList<Path>();
            var single = selected != null && selected.isSingleObject();
            if (single) {
                var source = failed.resolve(path.getFileName());
                checkNoSymlinks(outbox.toAbsolutePath().normalize(), source);
                if (Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
                    sources.add(source);
                }
            }
            else {
                try (var entries = Files.newDirectoryStream(failed)) {
                    for (var entry : entries) {
                        if (Files.isSymbolicLink(entry)) {
                            throw new IllegalArgumentException("Symbolic links are not allowed");
                        }
                        if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                            sources.add(entry);
                        }
                    }
                }
            }
            for (var source : sources) {
                var destination = single ? path : path.resolve(source.getFileName());
                checkNoSymlinks(root, destination);
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    return textResponse(Response.Status.CONFLICT, "Destination already exists: " + destination);
                }
            }
            for (var source : sources) {
                var destination = single ? path : path.resolve(source.getFileName());
                Files.createDirectories(destination.getParent());
                Files.move(source, destination);
                Files.deleteIfExists(failed.resolve(source.getFileName() + "-error.txt"));
            }
            return Response.ok().build();
        }
        catch (IllegalArgumentException e) {
            return textResponse(Response.Status.BAD_REQUEST, e.getMessage());
        }
        catch (FileAlreadyExistsException e) {
            return textResponse(Response.Status.CONFLICT, e.getMessage());
        }
        catch (IOException e) {
            return textResponse(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void checkNoSymlinks(Path root, Path path) {
        var current = root;
        if (Files.isSymbolicLink(current)) {
            throw new IllegalArgumentException("Symbolic links are not allowed");
        }
        for (var part : root.relativize(path)) {
            current = current.resolve(part);
            if (Files.isSymbolicLink(current)) {
                throw new IllegalArgumentException("Symbolic links are not allowed");
            }
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

    /**
     * Returns a plain text response. The content type is set explicitly, so that a message containing user-provided values (such as a path) is never
     * interpreted as HTML by a client, whatever media type it asks for.
     */
    private static Response textResponse(Response.Status status, String message) {
        return Response.status(status).entity(message).type(MediaType.TEXT_PLAIN_TYPE).build();
    }
}
