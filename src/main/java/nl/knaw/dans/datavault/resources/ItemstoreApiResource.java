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
import nl.knaw.dans.datavault.api.CopyFileToRequestDto;
import nl.knaw.dans.datavault.api.CreateDirectoryRequestDto;
import nl.knaw.dans.datavault.api.DeleteDirectoryRequestDto;
import nl.knaw.dans.datavault.api.DeleteFilesRequestDto;
import nl.knaw.dans.layerstore.ItemStore;

import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;

@Slf4j
@AllArgsConstructor
public class ItemstoreApiResource implements ItemstoreApi {
    private final ItemStore layeredItemStore;

    @Override
    public Response itemstoreCopyDirectoryIntoPost(CopyDirectoryIntoRequestDto copyDirectoryIntoRequestDto) {
        try {
            layeredItemStore.moveDirectoryInto(Paths.get(copyDirectoryIntoRequestDto.getSource()), copyDirectoryIntoRequestDto.getDestination());
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreCopyFileToPost(CopyFileToRequestDto copyFileToRequestDto) {
        try {
            try (var is = new FileInputStream(copyFileToRequestDto.getSource())) {
                layeredItemStore.writeFile(copyFileToRequestDto.getDestination(), is);
                log.debug("Copied file {} to item store at {}", copyFileToRequestDto.getSource(), copyFileToRequestDto.getDestination());
            }
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreCreateDirectoryPost(CreateDirectoryRequestDto createDirectoryRequestDto) {
        try {
            layeredItemStore.createDirectory(createDirectoryRequestDto.getPath());
            log.debug("Created directory in item store at {}", createDirectoryRequestDto.getPath());
            return Response.status(OK).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreDeleteDirectoryPost(DeleteDirectoryRequestDto deleteDirectoryRequestDto) {
        try {
            layeredItemStore.deleteDirectory(deleteDirectoryRequestDto.getPath());
            log.debug("Deleted directory from item store at {}", deleteDirectoryRequestDto.getPath());
            return Response.status(NO_CONTENT).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public Response itemstoreDeleteFilesPost(DeleteFilesRequestDto deleteFilesRequestDto) {
        try {
            layeredItemStore.deleteFiles(deleteFilesRequestDto.getPaths());
            log.debug("Deleted files from item store at {}", deleteFilesRequestDto.getPaths());
            return Response.status(NO_CONTENT).build();
        }
        catch (IllegalStateException e) {
            return Response.status(CONFLICT).build();
        }
        catch (IOException e) {
            return Response.status(INTERNAL_SERVER_ERROR).build();
        }
    }
}
