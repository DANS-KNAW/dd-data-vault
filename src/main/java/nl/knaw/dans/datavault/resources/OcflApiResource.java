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
import nl.knaw.dans.datavault.core.RepositoryProvider;

import javax.ws.rs.core.Response;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AllArgsConstructor
public class OcflApiResource implements OcflApi {
    private static final Pattern V_PREFIXED_NUMBER_PATTERN = Pattern.compile("v\\d+");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");
    private static final String LATEST_FLAG = "latest";

    private final RepositoryProvider ocflRepositoryProvider;

    @UnitOfWork
    @Override
    public Response ocflObjectsGet(Integer limit, Integer offset) {
        var ids = ocflRepositoryProvider.listObjectIds();
        var result = ids.stream()
            .skip(offset)
            .limit(limit)
            .collect(Collectors.toList());

        return Response.ok(result).build();
    }

    @UnitOfWork
    @Override
    public Response ocflObjectsIdDetailsGet(String id) {
        return ocflRepositoryProvider.describeObject(id)
            .map(Response::ok)
            .orElseGet(() -> Response.status(Response.Status.NOT_FOUND))
            .build();
    }

    @UnitOfWork
    @Override
    public Response ocflObjectsIdVersionsNrDetailsGet(String id, String nr) {
        var versionNumber = validateAndFormatVersionNumber(nr);

        if (versionNumber.isEmpty() && !LATEST_FLAG.equalsIgnoreCase(nr)) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Invalid version number").build();
        }

        return ocflRepositoryProvider.getVersionDetails(id, versionNumber.orElse(null))
            .map(Response::ok)
            .orElseGet(() -> Response.status(Response.Status.NOT_FOUND))
            .build();
    }

    @UnitOfWork
    @Override
    public Response ocflObjectsIdVersionsNrFilesGet(String id, String nr) {
        var versionNumber = validateAndFormatVersionNumber(nr);

        if (versionNumber.isEmpty() && !LATEST_FLAG.equalsIgnoreCase(nr)) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Invalid version number").build();
        }

        return ocflRepositoryProvider.listFiles(id, versionNumber.orElse(null))
            .map(Response::ok)
            .orElseGet(() -> Response.status(Response.Status.NOT_FOUND))
            .build();
    }

    private Optional<String> validateAndFormatVersionNumber(String nr) {
        if (LATEST_FLAG.equalsIgnoreCase(nr)) {
            return Optional.empty();
        }

        if (V_PREFIXED_NUMBER_PATTERN.matcher(nr).matches()) {
            return Optional.of(nr);
        }

        if (NUMBER_PATTERN.matcher(nr).matches()) {
            return Optional.of("v" + nr);
        }

        return Optional.empty();
    }
}
