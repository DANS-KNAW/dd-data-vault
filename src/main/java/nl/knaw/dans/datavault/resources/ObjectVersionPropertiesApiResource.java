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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.hibernate.UnitOfWork;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.core.RepositoryProvider;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Gets and sets object version properties, as defined by the DANS object-version-properties OCFL extension.
 */
@Slf4j
@AllArgsConstructor
public class ObjectVersionPropertiesApiResource implements ObjectVersionPropertiesApi {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RepositoryProvider repositoryProvider;

    @UnitOfWork
    @Override
    public Response objectVersionPropertiesIdNrGet(String id, String nr) {
        return handle(() -> Response.ok(repositoryProvider.getObjectVersionProperties(id, toOcflVersionOrNullForLatest(nr))).build());
    }

    @UnitOfWork
    @Override
    public Response objectVersionPropertiesIdNrNameGet(String id, String nr, String name) {
        return handle(() -> {
            var properties = repositoryProvider.getObjectVersionProperties(id, toOcflVersionOrNullForLatest(nr));
            if (!properties.containsKey(name)) {
                throw new NoSuchElementException("Property '" + name + "' not found in version " + nr + " of object " + id);
            }
            // Return a JsonNode, so that a string value is serialized as a JSON string and not as plain text
            return Response.ok(MAPPER.valueToTree(properties.get(name)), MediaType.APPLICATION_JSON_TYPE).build();
        });
    }

    @UnitOfWork
    @Override
    public Response objectVersionPropertiesIdNrNamePut(String id, String nr, String name, Object body) {
        return handle(() -> {
            if (body == null) {
                throw new IllegalArgumentException("The value of a property must not be null");
            }
            repositoryProvider.setObjectVersionProperty(id, toOcflVersion(nr), name, body);
            return Response.noContent().build();
        });
    }

    @UnitOfWork
    @Override
    public Response objectVersionPropertiesIdNrNameDelete(String id, String nr, String name) {
        return handle(() -> {
            repositoryProvider.deleteObjectVersionProperty(id, toOcflVersion(nr), name);
            return Response.noContent().build();
        });
    }

    private String toOcflVersionOrNullForLatest(String nr) {
        return VersionNumbers.isLatest(nr) ? null : toOcflVersion(nr);
    }

    private String toOcflVersion(String nr) {
        return VersionNumbers.toOcflFormat(nr)
            .orElseThrow(() -> new IllegalArgumentException("Invalid version number: " + nr));
    }

    private Response handle(Supplier<Response> action) {
        try {
            return action.get();
        }
        catch (NoSuchElementException e) {
            return error(NOT_FOUND, e);
        }
        catch (IllegalArgumentException e) {
            return error(BAD_REQUEST, e);
        }
        catch (IllegalStateException e) {
            return error(CONFLICT, e);
        }
    }

    private Response error(Response.Status status, RuntimeException e) {
        log.warn("Object version properties request failed with {}: {}", status, e.getMessage());
        return Response.status(status).entity(e.getMessage()).type(MediaType.TEXT_PLAIN_TYPE).build();
    }
}
