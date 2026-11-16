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
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerArchiver;

/**
 * A {@link LayerArchiver} that only records every archiving request as a {@link LayerArchiveJob} in the database. The jobs are executed by a polling task executor, using
 * {@link LayerArchiveJobTaskFactory}, which limits the number of parallel jobs. Because the database is the only source of truth, unfinished jobs are picked up automatically after a restart; the
 * archive action is therefore not used; only the layer id and the overwrite flag are persisted.
 */
@Slf4j
@RequiredArgsConstructor
public class DbBackedLayerArchiver implements LayerArchiver {
    private final LayerArchiveJobDao dao;

    @Override
    public void archive(long layerId, Runnable archiveAction) {
        archive(layerId, false, archiveAction);
    }

    @Override
    public void archive(long layerId, boolean overwrite, Runnable archiveAction) {
        var job = dao.register(layerId, overwrite);
        log.info("Layer {} scheduled for archiving in job {}", layerId, job.getId());
    }
}
