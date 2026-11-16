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
import nl.knaw.dans.datavault.db.LayerArchiveJobDao;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;

@RequiredArgsConstructor
public class LayerArchiveJobTaskFactory implements TaskFactory<LayerArchiveJob> {
    private final LayerArchiveJobDao dao;
    private final LayeredItemStore layeredItemStore;
    private final LayerConsistencyChecker consistencyChecker;

    @Override
    public Runnable create(LayerArchiveJob job) {
        return new LayerArchiveJobTask(job.getId(), job.getLayerId(), job.isOverwrite() || job.getAttempts() > 1, dao, layeredItemStore, consistencyChecker);
    }
}
