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
import nl.knaw.dans.datavault.core.ImportJob;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ImportJobDao extends AbstractDAO<ImportJob> implements TaskSource<ImportJob> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    public ImportJobDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public ImportJob create(ImportJob batch) {
        return super.persist(batch);
    }

    public ImportJob get(UUID id) {
        return super.get(id);
    }

    @Override
    public Optional<ImportJob> nextTask() {
        var criteria = currentSession().getCriteriaBuilder();
        var query = criteria.createQuery(ImportJob.class);
        var root = query.from(ImportJob.class);
        var statusPath = root.get("status");
        query.where(criteria.equal(statusPath, ImportJob.Status.PENDING));
        query.orderBy(criteria.asc(root.get("created")));

        return currentSession()
            .createQuery(query)
            .setMaxResults(1)
            .getResultStream()
            .findFirst();
    }

    public void update(ImportJob batch) {
        currentSession().update(batch);
    }

    public List<ImportJob> list() {
        var criteria = currentSession().getCriteriaBuilder();
        var query = criteria.createQuery(ImportJob.class);
        var root = query.from(ImportJob.class);
        query.orderBy(criteria.asc(root.get("created")));
        return currentSession().createQuery(query).getResultList();
    }
}
