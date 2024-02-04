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
package nl.knaw.dans.datavault;

import nl.knaw.dans.datavault.api.JobDto;
import nl.knaw.dans.datavault.core.Job;
import org.mapstruct.Mapper;

import java.nio.file.Path;
import java.util.UUID;

/**
 * nl.knaw.dans.datavault.Conversions between DTOs and domain objects.
 */
@Mapper
public interface Conversions {
    Job convert(JobDto parameters);

    default UUID stringToUuid(String value) {
        if (value == null) {
            return null;
        }
        return UUID.fromString(value);
    }

    default String uuidToString(UUID value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    default String pathToString(Path path) {
        if (path == null) {
            return null;
        }
        return path.toString();
    }

    default Path stringToPath(String path) {
        if (path == null) {
            return null;
        }
        return Path.of(path);
    }

}
