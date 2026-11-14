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
 * Persists {@link LayerArchiveJob}s. The public methods declare their own {@link UnitOfWork}, so the DAO must be created with a {@code UnitOfWorkAwareProxyFactory}. This makes it usable from
 * inside and outside other units of work, for example from the API resources and from long-running worker threads that must not hold a database connection.
 */
public class LayerArchiveJobDao extends AbstractDAO<LayerArchiveJob> implements TaskSource<LayerArchiveJob> {
    private final int maxParallelJobs;

    public LayerArchiveJobDao(SessionFactory sessionFactory, int maxParallelJobs) {
        super(sessionFactory);
        this.maxParallelJobs = maxParallelJobs;
    }

    /**
     * Registers a job for the layer, unless there is already an unfinished one.
     *
     * @param layerId the layer to archive
     * @return the new or already existing unfinished job
     */
    @UnitOfWork
    public synchronized LayerArchiveJob register(long layerId) {
        return findActive(layerId).orElseGet(() -> persist(LayerArchiveJob.builder()
            .layerId(layerId)
            .created(OffsetDateTime.now())
            .status(Status.PENDING)
            .build()));
    }

    @UnitOfWork
    public Optional<LayerArchiveJob> find(UUID id) {
        return Optional.ofNullable(get(id));
    }

    @UnitOfWork
    public List<LayerArchiveJob> findByStatus(Status status) {
        return jobQuery("from layer_archive_job where status = :status order by created").setParameter("status", status).getResultList();
    }

    @UnitOfWork
    public List<LayerArchiveJob> list() {
        return jobQuery("from layer_archive_job order by created").getResultList();
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
        var running = currentSession().createQuery("select count(j) from layer_archive_job j where j.status = :status", Long.class)
            .setParameter("status", Status.RUNNING)
            .getSingleResult();
        var free = (int) Math.min(limit, Math.max(0, maxParallelJobs - running));
        if (free == 0) {
            return List.of();
        }
        var jobs = jobQuery("from layer_archive_job where status = :status order by created")
            .setParameter("status", Status.PENDING)
            .setMaxResults(free)
            .getResultList();
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
     * Resets jobs that were RUNNING when the service stopped to PENDING.
     *
     * @return all pending jobs, oldest first
     */
    @UnitOfWork
    public synchronized List<LayerArchiveJob> resetInterruptedJobs() {
        jobQuery("from layer_archive_job where status = :status")
            .setParameter("status", Status.RUNNING)
            .getResultList()
            .forEach(job -> {
                job.setStatus(Status.PENDING);
                job.setStarted(null);
                job.setMessage("Interrupted by a restart of the service");
            });
        return jobQuery("from layer_archive_job where status = :status order by created")
            .setParameter("status", Status.PENDING)
            .getResultList();
    }

    private Optional<LayerArchiveJob> findActive(long layerId) {
        return jobQuery("from layer_archive_job where layerId = :layerId and status in (:statuses) order by created")
            .setParameter("layerId", layerId)
            .setParameterList("statuses", List.of(Status.PENDING, Status.RUNNING))
            .setMaxResults(1)
            .uniqueResultOptional();
    }

    private Query<LayerArchiveJob> jobQuery(String hql) {
        return currentSession().createQuery(hql, LayerArchiveJob.class);
    }
}
