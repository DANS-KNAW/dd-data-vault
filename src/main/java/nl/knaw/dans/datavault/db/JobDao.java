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
import javax.persistence.criteria.Root;
import java.util.List;

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

    public List<Job> getNextJobs(int number) {
        CriteriaQuery<Job> query = criteriaQuery();
        CriteriaBuilder cb = currentSession().getCriteriaBuilder();
        Root<Job> root = query.from(Job.class);
        query.where(cb.equal(root.get("finished"), false));
        query.orderBy(cb.asc(root.get("creationTimestamp")));
        query.select(root);

        return currentSession().createQuery(query)
            .setMaxResults(number)
            .getResultList();
    }

}

