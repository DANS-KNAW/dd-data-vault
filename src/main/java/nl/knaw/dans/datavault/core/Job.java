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

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import java.nio.file.Path;
import java.util.UUID;

// TODO: add startedTimestamp, finishedTimestamp, status, etc.
@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Job {
    public enum State {
        WAITING,
        RUNNING,
        SUCCESS,
        FAIL
    }

    @PrePersist
    protected void onCreate() {
        creationTimestamp = System.currentTimeMillis();
        state = State.WAITING;
    }

    @PreUpdate
    protected void onUpdate() {
        modificationTimestamp = System.currentTimeMillis();
    }

    @Id
    @GeneratedValue
    @Column(name = "uuid", columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(name = "creation_timestamp")
    private long creationTimestamp;

    @Column(name = "modification_timestamp")
    private long modificationTimestamp;

    @Column(name = "state")
    @Enumerated(EnumType.STRING)
    private State state;

    @Convert(converter = PathConverter.class)
    @Column(name = "batch")
    private Path batch;
}
