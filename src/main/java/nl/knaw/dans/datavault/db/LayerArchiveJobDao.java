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
import io.dropwizard.hibernate.UnitOfWork;
import nl.knaw.dans.datavault.core.LayerArchiveJob;
import nl.knaw.dans.datavault.core.LayerArchiveJob.Status;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists {@link LayerArchiveJob}s. The public methods declare their own {@link UnitOfWork}, so the DAO must be created with a {@code UnitOfWorkAwareProxyFactory}. This makes it usable from inside
 * and outside other units of work, for example from the API resources and from long-running worker threads that must not hold a database connection.
 */
public class LayerArchiveJobDao extends AbstractDAO<LayerArchiveJob> implements TaskSource<LayerArchiveJob> {
    private final int maxParallelJobs;

    public LayerArchiveJobDao(SessionFactory sessionFactory, int maxParallelJobs) {
        super(sessionFactory);
        this.maxParallelJobs = maxParallelJobs;
    }

    /**
     * Registers a job for the layer, unless there is already an unfinished one. If there is one, it will overwrite an existing archive when either request asked for that.
     *
     * @param layerId   the layer to archive
     * @param overwrite whether an existing archive must be overwritten
     * @return the new or already existing unfinished job
     */
    @UnitOfWork
    public synchronized LayerArchiveJob register(long layerId, boolean overwrite) {
        var active = findActive(layerId);
        if (active.isPresent()) {
            var job = active.get();
            job.setOverwrite(job.isOverwrite() || overwrite);
            return job;
        }
        return persist(LayerArchiveJob.builder()
            .layerId(layerId)
            .activeLayerId(layerId)
            .overwrite(overwrite)
            .created(OffsetDateTime.now())
            .status(Status.PENDING)
            .build());
    }

    @UnitOfWork
    public Optional<LayerArchiveJob> find(UUID id) {
        return Optional.ofNullable(get(id));
    }

    /**
     * Finds the most recently created job for a layer, using the job ID to break timestamp ties.
     */
    @UnitOfWork
    public Optional<LayerArchiveJob> findLatestByLayer(long layerId) {
        var criteria = currentSession().getCriteriaBuilder();
        var query = criteria.createQuery(LayerArchiveJob.class);
        var root = query.from(LayerArchiveJob.class);
        query.where(criteria.equal(root.get("layerId"), layerId));
        query.orderBy(criteria.desc(root.get("created")), criteria.desc(root.get("id")));
        return currentSession().createQuery(query).setMaxResults(1).uniqueResultOptional();
    }

    @UnitOfWork
    public List<LayerArchiveJob> findByStatus(Status status) {
        return jobsByStatus(status).getResultList();
    }

    @UnitOfWork
    public List<LayerArchiveJob> list() {
        return jobsByStatus(null).getResultList();
    }

    @Override
    @UnitOfWork
    public synchronized Optional<LayerArchiveJob> nextInput() {
        return claim(1).stream().findFirst();
    }

    @Override
    @UnitOfWork
    public synchronized List<LayerArchiveJob> nextInputs() {
        return claim(Integer.MAX_VALUE);
    }

    /**
     * Marks the oldest pending jobs as RUNNING, so that at most maxParallelJobs jobs are running (or waiting for a worker thread) at any time.
     */
    private List<LayerArchiveJob> claim(int limit) {
        var criteria = currentSession().getCriteriaBuilder();
        var countQuery = criteria.createQuery(Long.class);
        var countRoot = countQuery.from(LayerArchiveJob.class);
        countQuery.select(criteria.count(countRoot)).where(criteria.equal(countRoot.get("status"), Status.RUNNING));
        var running = currentSession().createQuery(countQuery).getSingleResult();
        var free = (int) Math.min(limit, Math.max(0, maxParallelJobs - running));
        if (free == 0) {
            return List.of();
        }
        var jobs = jobsByStatus(Status.PENDING).setMaxResults(free).getResultList();
        for (var job : jobs) {
            job.setStatus(Status.RUNNING);
            job.setStarted(OffsetDateTime.now());
            job.setAttempts(job.getAttempts() + 1);
        }
        return jobs;
    }

    @UnitOfWork
    public void finish(UUID id, Status status, String message) {
        var job = get(id);
        job.setStatus(status);
        job.setActiveLayerId(null);
        job.setFinished(OffsetDateTime.now());
        job.setMessage(message);
    }

    /**
     * Puts a running job back to PENDING without counting the attempt.
     */
    @UnitOfWork
    public void requeue(UUID id, String message) {
        var job = get(id);
        job.setStatus(Status.PENDING);
        job.setStarted(null);
        job.setAttempts(Math.max(0, job.getAttempts() - 1));
        job.setMessage(message);
    }

    /**
     * Resets jobs that were RUNNING when the service stopped to PENDING, so that the polling task executor picks them up again. Must be called at startup, before the executor is started.
     */
    @UnitOfWork
    public synchronized void resetInterruptedJobs() {
        jobsByStatus(Status.RUNNING).getResultList().forEach(job -> {
            job.setStatus(Status.PENDING);
            job.setStarted(null);
            job.setMessage("Interrupted by a restart of the service");
        });
    }

    private Optional<LayerArchiveJob> findActive(long layerId) {
        var criteria = currentSession().getCriteriaBuilder();
        var query = criteria.createQuery(LayerArchiveJob.class);
        var root = query.from(LayerArchiveJob.class);
        query.where(criteria.equal(root.get("activeLayerId"), layerId));
        return currentSession().createQuery(query).uniqueResultOptional();
    }

    private Query<LayerArchiveJob> jobsByStatus(Status status) {
        var criteria = currentSession().getCriteriaBuilder();
        var query = criteria.createQuery(LayerArchiveJob.class);
        var root = query.from(LayerArchiveJob.class);
        if (status != null) {
            query.where(criteria.equal(root.get("status"), status));
        }
        query.orderBy(criteria.asc(root.get("created")));
        return currentSession().createQuery(query);
    }
}
