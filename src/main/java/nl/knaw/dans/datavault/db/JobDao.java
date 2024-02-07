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
import nl.knaw.dans.datavault.core.Job;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.Root;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class JobDao extends AbstractDAO<Job> {
    public JobDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public Job create(Job job) {
        return persist(job);
    }

    public List<Job> findAll() {
        CriteriaQuery<Job> query = criteriaQuery();
        query.from(Job.class);
        return list(query);
    }

    public Optional<Job> getNextJob() {
        CriteriaQuery<Job> query = criteriaQuery();
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        Root<Job> root = query.from(Job.class);
        query.where(cb.equal(root.get("state"), Job.State.WAITING));
        query.orderBy(cb.asc(root.get("creationTimestamp")));
        query.select(root);

        return currentSession().createQuery(query)
            .setMaxResults(1)
            .uniqueResultOptional();
    }

    public void updateState(UUID id, Job.State state) {
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        CriteriaUpdate<Job> update = cb.createCriteriaUpdate(Job.class);
        Root<Job> root = update.from(Job.class);

        update.set(root.get("state"), state);
        update.where(cb.equal(root.get("id"), id));

        currentSession().createQuery(update).executeUpdate();
    }
}

