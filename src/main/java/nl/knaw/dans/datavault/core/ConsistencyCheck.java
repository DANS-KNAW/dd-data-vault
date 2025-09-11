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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;


@Entity
@Table(name = "consistency_check")
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ConsistencyCheck {
    public enum Type {
        LAYER_IDS, LISTING_RECORDS
    }

    public enum Result {
        OK, NOT_OK, ERROR
    }

    @Id
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
    @Column(name = "id", nullable = false/*, columnDefinition = "uuid"*/)
    private UUID id;

    @Column(nullable = false)
    private Type type;

    @Column(name = "layer_id")
    private long layerId;

    @Column(name = "created", nullable = false)
    private OffsetDateTime created;

    @Column(name = "started")
    private OffsetDateTime started;

    @Column(name = "finished")
    private OffsetDateTime finished;

    @Column(name = "result")
    private Result result;

    @Column(name = "message")
    @Lob
    private String message;

}
