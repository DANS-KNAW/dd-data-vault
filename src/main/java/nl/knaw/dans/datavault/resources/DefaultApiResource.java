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

import nl.knaw.dans.datavault.api.AppInfoDto;

import javax.ws.rs.core.Response;

public class DefaultApiResource implements DefaultApi {
    @Override
    public Response rootGet(String accept) {
        if ("text/plain".equals(accept)) {
            return Response.ok("Welcome to the Data Vault API!").build();
        }
        else {
            return Response.ok(new AppInfoDto().name("dd-data-vault")
                    .version(this.getClass().getPackage().getImplementationVersion()))
                .build();
        }

    }
}
