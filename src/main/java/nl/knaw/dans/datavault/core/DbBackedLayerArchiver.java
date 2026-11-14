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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.core.LayerArchiveJob.Status;
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerArchiver;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link LayerArchiver} that records every archiving request as a {@link LayerArchiveJob} in the database. The jobs are executed by a polling task executor (see {@link #taskFactory()}), which
 * limits the number of parallel jobs. After a restart, {@link #resumeUnfinishedJobs(LayeredItemStore)} re-registers the archive actions of unfinished jobs.
 */
@Slf4j
@RequiredArgsConstructor
public class DbBackedLayerArchiver implements LayerArchiver {
    private final LayerArchiveJobDao dao;
    private final LayerConsistencyChecker consistencyChecker;
    private final Map<Long, Runnable> actions = new ConcurrentHashMap<>();

    @Override
    public void archive(long layerId, Runnable archiveAction) {
        actions.put(layerId, archiveAction);
        var job = dao.register(layerId);
        log.info("Layer {} scheduled for archiving in job {}", layerId, job.getId());
    }

    public TaskFactory<LayerArchiveJob> taskFactory() {
        return job -> () -> execute(job.getId(), job.getLayerId());
    }

    /**
     * Must be called once at startup, before the jobs are executed. Jobs that were interrupted by a restart are made PENDING again and their archive actions are re-created by requesting the archiving
     * of the layers again.
     *
     * @param layeredItemStore the item store to request the archiving from
     */
    public void resumeUnfinishedJobs(LayeredItemStore layeredItemStore) {
        for (var job : dao.resetInterruptedJobs()) {
            log.info("Resuming archive job {} for layer {}", job.getId(), job.getLayerId());
            try {
                // Overwrite is always allowed, because an earlier attempt may have left an archive behind
                layeredItemStore.archiveLayer(job.getLayerId(), true);
            }
            catch (Exception e) {
                // Archiving was probably completed just before the restart, so the layer is no longer closed
                if (e.getMessage() != null && e.getMessage().contains("is in state ARCHIVED")) {
                    log.info("Layer {} was already archived", job.getLayerId());
                    dao.finish(job.getId(), Status.SUCCESS, "Layer was already archived");
                }
                else {
                    log.error("Cannot resume archiving of layer {}", job.getLayerId(), e);
                    dao.finish(job.getId(), Status.FAILED, "Cannot resume: " + e.getMessage());
                }
            }
        }
    }

    void execute(UUID jobId, long layerId) {
        var action = actions.get(layerId);
        if (action == null) {
            log.warn("No archive action known for layer {}; leaving job {} pending", layerId, jobId);
            dao.requeue(jobId, "Waiting for the archive action to be registered");
            return;
        }
        try {
            log.info("Checking consistency of layer {}", layerId);
            consistencyChecker.check(layerId);
            log.info("Archiving layer {}", layerId);
            action.run();
            log.info("Layer {} archived", layerId);
            dao.finish(jobId, Status.SUCCESS, null);
            actions.remove(layerId, action);
        }
        catch (MoratoriumException e) {
            log.warn("Archiving of layer {} postponed: {}", layerId, e.getMessage());
            dao.requeue(jobId, e.getMessage());
        }
        catch (Exception e) {
            log.error("Archiving of layer {} failed", layerId, e);
            dao.finish(jobId, Status.FAILED, e.getClass().getName() + ": " + e.getMessage());
            actions.remove(layerId, action);
        }
    }
}
