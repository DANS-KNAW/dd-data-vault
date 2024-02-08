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
import nl.knaw.dans.datavault.api.JobDto;
import nl.knaw.dans.datavault.core.InvalidJobException;
import nl.knaw.dans.datavault.core.JobService;

import javax.ws.rs.core.Response;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.CREATED;

@AllArgsConstructor
public class JobsApiResource implements JobsApi {
    private final JobService jobService;

    @Override
    @UnitOfWork
    public Response jobsGet() {
        return null;
    }

    @Override
    public Response jobsIdGet(UUID id) {
        return null;
    }

    @Override
    @UnitOfWork
    public Response jobsPost(JobDto jobDto) {
        try {
            jobService.startJob(jobDto);
            return Response.status(CREATED).entity(jobDto).build();
        }
        catch (InvalidJobException e) {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }
}
