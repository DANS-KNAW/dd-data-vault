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
package nl.knaw.dans.datavault.db;

import io.dropwizard.hibernate.AbstractDAO;
import nl.knaw.dans.datavault.core.ConsistencyCheck;
import org.hibernate.SessionFactory;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

public class ConsistencyCheckDao extends AbstractDAO<ConsistencyCheck> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public ConsistencyCheckDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public ConsistencyCheck create(ConsistencyCheck check) {
        return super.persist(check);
    }

    public ConsistencyCheck get(UUID id) {
        return super.get(id);
    }

    public ConsistencyCheck start(ConsistencyCheck check) {
        check.setStarted(OffsetDateTime.now());
        return super.persist(check);
    }

    public ConsistencyCheck finish(ConsistencyCheck check, ConsistencyCheck.Result result, String message) {
        check.setFinished(OffsetDateTime.now());
        check.setResult(result);
        check.setMessage(message);
        return super.persist(check);
    }

    public Optional<ConsistencyCheck> findOldestUnfinished() {
        var criteriaBuilder = currentSession().getCriteriaBuilder();
        var criteriaQuery = criteriaBuilder.createQuery(ConsistencyCheck.class);
        var root = criteriaQuery.from(ConsistencyCheck.class);
        criteriaQuery.select(root)
            .where(
                criteriaBuilder.and(
                    criteriaBuilder.isNull(root.get("finished")),
                    criteriaBuilder.lessThan(root.get("created"), OffsetDateTime.now().minusMinutes(1))
                )
            )
            .orderBy(criteriaBuilder.asc(root.get("created")));
        return currentSession()
            .createQuery(criteriaQuery)
            .setMaxResults(1)
            .uniqueResultOptional();
    }
}
