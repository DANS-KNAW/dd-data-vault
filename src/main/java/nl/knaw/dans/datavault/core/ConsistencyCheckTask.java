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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.core.ConsistencyCheck.Result;
import nl.knaw.dans.datavault.core.ConsistencyCheck.Type;
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import nl.knaw.dans.layerstore.ItemsMismatchException;
import nl.knaw.dans.layerstore.LayerIdsMismatchException;
import nl.knaw.dans.layerstore.LayeredItemStore;

import java.io.IOException;

/**
 * Defines a UnitOfWork to perform a consistency check on the repository.
 */
@RequiredArgsConstructor
@Slf4j
public class ConsistencyCheckTask implements Runnable {
    private final ConsistencyCheckDao consistencyCheckDao;
    private final ConsistencyCheck consistencyCheck;
    private final LayeredItemStore layeredItemStore;

    @Override
    @UnitOfWork
    public void run() {
        consistencyCheckDao.start(consistencyCheck);
        if (consistencyCheck.getType().equals(ConsistencyCheck.Type.LAYER_IDS)) {
            log.debug("Checking consistency of layer IDs on storage and database");
            try {
                layeredItemStore.checkSameLayersOnStorageAndDb();
                consistencyCheckDao.finish(consistencyCheck, Result.OK, null);
                log.debug("Consistency check passed");
            }
            catch (IOException e) {
                log.error("Error checking layer IDs", e);
                consistencyCheckDao.finish(consistencyCheck, Result.ERROR, e.getMessage());
            }
            catch (LayerIdsMismatchException e) {
                log.error("Layer IDs mismatch", e);
                consistencyCheckDao.finish(consistencyCheck, Result.NOT_OK, e.getMessage());
            }
        }
        else if (consistencyCheck.getType().equals(Type.LISTING_RECORDS)) {
            log.debug("Checking consistency of listing records for layer {}", consistencyCheck.getLayerId());
            try {
                layeredItemStore.checkLayerItemRecords(consistencyCheck.getLayerId());
                consistencyCheckDao.finish(consistencyCheck, Result.OK, null);
                log.debug("Consistency check passed");
            }
            catch (IOException e) {
                log.error("Error checking layer IDs", e);
                consistencyCheckDao.finish(consistencyCheck, Result.ERROR, e.getMessage());
            }
            catch (ItemsMismatchException e) {
                log.error("Listing records mismatch", e);
                consistencyCheckDao.finish(consistencyCheck, Result.NOT_OK, e.getMessage());
            }
        }
    }
}
