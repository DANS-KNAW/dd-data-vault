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
import io.dropwizard.lifecycle.Managed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.db.ConsistencyCheckDao;
import nl.knaw.dans.layerstore.LayeredItemStore;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class ConsistencyCheckExecutor implements Managed {
    private final ScheduledExecutorService scheduler;
    private final ConsistencyCheckDao consistencyCheckDao;
    private final LayeredItemStore layeredItemStore;
    private final long pollIntervalSeconds;

    private ScheduledFuture<?> future;

    @Override
    public void start() {
        future = scheduler.scheduleWithFixedDelay(this::tick, 0, pollIntervalSeconds, TimeUnit.SECONDS);
        log.info("ConsistencyCheckScheduler started; polling every {} seconds", pollIntervalSeconds);
    }

    @Override
    public void stop() {
        if (future != null) {
            future.cancel(false);
        }
        scheduler.shutdown();
        log.info("ConsistencyCheckScheduler stopped");
    }

    @UnitOfWork
    public void tick() {
        try {
            Optional<ConsistencyCheck> next = findOldestUnfinished();
            if (next.isEmpty()) {
                return;
            }
            log.debug("Found next unfinished consistency check: {}", next.get());

            var task = new ConsistencyCheckTask(consistencyCheckDao, next.get(), layeredItemStore);
            task.run();
        }
        catch (Exception e) {
            log.error("Error while polling or running consistency check task", e);
        }
    }

    private Optional<ConsistencyCheck> findOldestUnfinished() {
        return consistencyCheckDao.findOldestUnfinished();
    }
}
