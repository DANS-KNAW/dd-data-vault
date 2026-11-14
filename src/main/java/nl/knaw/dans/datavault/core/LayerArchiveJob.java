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
package nl.knaw.dans.datavault.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A request to archive a layer. Jobs survive a restart of the service, so that unfinished jobs can be resumed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity(name = "layer_archive_job")
public class LayerArchiveJob {
    public enum Status {
        PENDING,
        RUNNING,
        SUCCESS,
        FAILED
    }

    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
    @Column(nullable = false)
    private UUID id;

    @Column(nullable = false)
    private long layerId;

    @Column(nullable = false)
    private OffsetDateTime created;

    @Column
    private OffsetDateTime started;

    @Column
    private OffsetDateTime finished;

    @Column(nullable = false)
    private Status status;

    @Column
    private int attempts;

    @Column
    @Lob
    private String message;
}
