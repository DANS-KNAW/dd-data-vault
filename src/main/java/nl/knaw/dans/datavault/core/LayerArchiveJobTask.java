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
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;

import java.util.UUID;

/**
 * Executes one {@link LayerArchiveJob}. An existing archive is only overwritten when a rearchive was requested or when this is a retry of an interrupted job, as the earlier attempt may have left a
 * partial archive behind. The task does not hold a database session while archiving; the DAO and the consistency checker declare their own units of work.
 */
@Slf4j
@RequiredArgsConstructor
public class LayerArchiveJobTask implements Runnable {
    private final UUID jobId;
    private final long layerId;
    private final boolean overwrite;
    private final LayerArchiveJobDao dao;
    private final LayeredItemStore layeredItemStore;
    private final LayerConsistencyChecker consistencyChecker;

    @Override
    public void run() {
        try {
            if (layeredItemStore.isLayerArchived(layerId)) {
                // Archiving was probably completed just before a restart
                log.info("Layer {} was already archived", layerId);
                dao.finish(jobId, Status.SUCCESS, "Layer was already archived");
                return;
            }
            log.info("Checking consistency of layer {}", layerId);
            consistencyChecker.check(layerId);
            log.info("Archiving layer {}", layerId);
            layeredItemStore.archiveLayerDirectly(layerId, overwrite);
            log.info("Layer {} archived", layerId);
            dao.finish(jobId, Status.SUCCESS, null);
        }
        catch (MoratoriumException e) {
            log.warn("Archiving of layer {} postponed: {}", layerId, e.getMessage());
            dao.requeue(jobId, e.getMessage());
        }
        catch (Exception e) {
            log.error("Archiving of layer {} failed", layerId, e);
            dao.finish(jobId, Status.FAILED, e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
