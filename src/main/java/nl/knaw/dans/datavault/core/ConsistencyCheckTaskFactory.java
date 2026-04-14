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
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import nl.knaw.dans.layerstore.LayeredItemStore;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskFactory;

import java.util.List;

@RequiredArgsConstructor
public class ConsistencyCheckTaskFactory implements TaskFactory<ConsistencyCheck> {
    private final ConsistencyCheckDao dao;
    private final LayeredItemStore layeredItemStore;

    @Override
    public Runnable create(List<ConsistencyCheck> records) {
        if (records.size() != 1) {
            throw new IllegalArgumentException("Exactly one consistency check job expected");
        }
        return new ConsistencyCheckTask(dao, records.get(0), layeredItemStore);
    }
}
