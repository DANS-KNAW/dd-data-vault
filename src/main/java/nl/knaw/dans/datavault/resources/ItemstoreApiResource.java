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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.api.CopyDirectoryIntoRequestDto;
import nl.knaw.dans.datavault.api.CopyFileIntoRequestDto;
import nl.knaw.dans.datavault.api.CopyFileOutOfRequestDto;
import nl.knaw.dans.datavault.api.CreateDirectoryRequestDto;
import nl.knaw.dans.datavault.api.DeleteDirectoryRequestDto;
import nl.knaw.dans.datavault.api.DeleteFilesRequestDto;
import nl.knaw.dans.datavault.config.ItemstoreConfig;
import nl.knaw.dans.layerstore.ItemStore;
import org.apache.commons.io.FileUtils;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;

@Slf4j
@AllArgsConstructor
public class ItemstoreApiResource implements ItemstoreApi {
    private final ItemStore layeredItemStore;
    private final ItemstoreConfig itemstoreConfig;

    @Override
    public Response itemstoreCopyDirectoryIntoPost(CopyDirectoryIntoRequestDto copyDirectoryIntoRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getCopyDirectoryInto())) {
            log.warn("End-point called while disabled: itemstoreCopyDirectoryIntoPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            var source = exchangeDir().existingDirectoryWithoutSymlinks(copyDirectoryIntoRequestDto.getSource(), "Source");
            var stagingDir = Paths.get(itemstoreConfig.getWorkDir()).resolve(UUID.randomUUID().toString());
            Files.createDirectories(stagingDir);
            try {
                FileUtils.copyDirectory(source.toFile(), stagingDir.toFile());
                layeredItemStore.moveDirectoryInto(stagingDir, removeLeadingSlashes(copyDirectoryIntoRequestDto.getDestination()));
            }
            finally {
                FileUtils.deleteQuietly(stagingDir.toFile());
            }
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    private ExchangeDir exchangeDir() {
        return new ExchangeDir(itemstoreConfig.getExchangeDir());
    }

    private Response badRequest(String message) {
        // Explicit text/plain, so that a message is never interpreted as HTML by a client
        return Response.status(BAD_REQUEST).entity(message).type(MediaType.TEXT_PLAIN_TYPE).build();
    }

    private String removeLeadingSlashes(String path) {
        return path.stripLeading().replaceFirst("^/+", "").trim();
    }

    @Override
    public Response itemstoreCopyFileIntoPost(CopyFileIntoRequestDto copyFileIntoRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getCopyFileInto())) {
            log.warn("End-point called while disabled: itemstoreCopyFileToPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            var source = exchangeDir().existingFile(copyFileIntoRequestDto.getSource(), "Source");
            try (var is = Files.newInputStream(source)) {
                layeredItemStore.writeFile(removeLeadingSlashes(copyFileIntoRequestDto.getDestination()), is);
                log.debug("Copied file {} to item store at {}", copyFileIntoRequestDto.getSource(), copyFileIntoRequestDto.getDestination());
            }
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreCopyFileOutOfPost(CopyFileOutOfRequestDto copyFileOutOfRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getCopyFileOutOf())) {
            log.warn("End-point called while disabled: itemstoreCopyFileOutOfPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            var destination = exchangeDir().newFile(copyFileOutOfRequestDto.getDestination(), "Destination");
            try (var is = layeredItemStore.readFile(removeLeadingSlashes(copyFileOutOfRequestDto.getSource()))) {
                Files.copy(is, destination);
                log.debug("Copied file {} from item store to {}", copyFileOutOfRequestDto.getSource(), copyFileOutOfRequestDto.getDestination());
            }
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreCreateDirectoryPost(CreateDirectoryRequestDto createDirectoryRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getCreateDirectory())) {
            log.debug("End-point called while disabled: itemstoreCreateDirectoryPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            layeredItemStore.createDirectories(removeLeadingSlashes(createDirectoryRequestDto.getPath()));
            log.debug("Created directory in item store at {}", createDirectoryRequestDto.getPath());
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreDeleteDirectoryPost(DeleteDirectoryRequestDto deleteDirectoryRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getDeleteDirectory())) {
            log.warn("End-point called while disabled: itemstoreDeleteDirectoryPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            layeredItemStore.deleteDirectory(removeLeadingSlashes(deleteDirectoryRequestDto.getPath()));
            log.debug("Deleted directory from item store at {}", deleteDirectoryRequestDto.getPath());
            return Response.status(NO_CONTENT).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreDeleteFilesPost(DeleteFilesRequestDto deleteFilesRequestDto) {
        if (!Boolean.TRUE.equals(itemstoreConfig.getEnableEndpoints().getDeleteFiles())) {
            log.warn("End-point called while disabled: itemstoreDeleteFilesPost");
            return Response.status(Response.Status.FORBIDDEN).build();
        }
        try {
            layeredItemStore.deleteFiles(deleteFilesRequestDto.getPaths().stream().map(this::removeLeadingSlashes).toList());
            log.debug("Deleted files from item store at {}", deleteFilesRequestDto.getPaths());
            return Response.status(NO_CONTENT).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IllegalArgumentException e) {
            return badRequest(e.getMessage());
        }
        catch (IOException e) {
            log.error("Internal server error", e);
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }
}
