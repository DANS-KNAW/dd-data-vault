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
import nl.knaw.dans.datavault.core.VaultObject;
import org.hibernate.SessionFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class VaultObjectDao extends AbstractDAO<VaultObject> {

    public VaultObjectDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public VaultObject save(VaultObject vaultObject) {
        return persist(vaultObject);
    }

    public void saveAll(Collection<VaultObject> vaultObjects) {
        for (var vaultObject : vaultObjects) {
            persist(vaultObject);
        }
    }

    public Optional<VaultObject> findByObjectId(String objectId) {
        var cb = currentSession().getCriteriaBuilder();
        var cq = cb.createQuery(VaultObject.class);
        var root = cq.from(VaultObject.class);
        cq.where(cb.equal(root.get("objectId"), objectId));

        return currentSession().createQuery(cq).getResultStream().findFirst();
    }

    public boolean existsByObjectId(String objectId) {
        var cb = currentSession().getCriteriaBuilder();
        var cq = cb.createQuery(Long.class);
        var root = cq.from(VaultObject.class);
        cq.select(cb.count(root));
        cq.where(cb.equal(root.get("objectId"), objectId));

        return currentSession().createQuery(cq).getSingleResult() > 0;
    }

    public List<String> findObjectIds(Integer limit, Integer offset) {
        var cb = currentSession().getCriteriaBuilder();
        var cq = cb.createQuery(String.class);
        var root = cq.from(VaultObject.class);
        cq.select(root.get("objectId"));
        cq.orderBy(cb.asc(root.get("objectId")));

        var query = currentSession().createQuery(cq);
        if (offset != null && offset > 0) {
            query.setFirstResult(offset);
        }
        if (limit != null && limit >= 0) {
            query.setMaxResults(limit);
        }
        return query.getResultList();
    }

    public long count() {
        var cb = currentSession().getCriteriaBuilder();
        var cq = cb.createQuery(Long.class);
        var root = cq.from(VaultObject.class);
        cq.select(cb.count(root));

        return currentSession().createQuery(cq).getSingleResult();
    }
}
