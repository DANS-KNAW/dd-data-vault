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
import nl.knaw.dans.datavault.Conversions;
import nl.knaw.dans.datavault.api.ConsistencyCheckRequestDto;
import nl.knaw.dans.datavault.core.ConsistencyCheck;
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import org.mapstruct.factory.Mappers;

import javax.ws.rs.core.Response;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@RequiredArgsConstructor
public class ConsistencyChecksApiResource implements ConsistencyChecksApi {
    private final Conversions conversions = Mappers.getMapper(Conversions.class);
    private final ConsistencyCheckDao consistencyCheckDao;

    @Override
    @UnitOfWork
    public Response consistencyChecksIdGet(UUID id) {
        return null;
    }

    @Override
    @UnitOfWork
    public Response consistencyChecksPost(ConsistencyCheckRequestDto consistencyCheckRequestDto) {
        var consistencyCheck = new ConsistencyCheck();
        consistencyCheck.setCreated(OffsetDateTime.now(ZoneOffset.UTC));
        consistencyCheck.setType(conversions.convert(consistencyCheckRequestDto.getType()));
        if (consistencyCheckRequestDto.getLayerId() != null) {
            consistencyCheck.setLayerId(consistencyCheckRequestDto.getLayerId());
        }
        return Response.accepted(conversions.convert(consistencyCheckDao.create(consistencyCheck))).build();
    }
}
