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
import nl.knaw.dans.datavault.db.TaskRecordDao;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * <p>Generic serial background executor that:</p>
 * <ul>
 * <li>periodically polls for the next task record via <code>TaskRecordDao#nextTask()</code></li>
 * <li>creates a <code>Runnable</code> via <code>TaskFactory#create(record)</code></li>
 * <li>runs the task synchronously (one at a time)</li>
 * </ul>
 *
 * @param <R> record type
 */
@Slf4j
@RequiredArgsConstructor
public class SerialBackgroundExecutor<R> implements Managed {
    private final ScheduledExecutorService scheduler;
    private final TaskRecordDao<R> taskRecordDao;
    private final TaskFactory<R> taskFactory;
    private final Duration pollingInterval;
    private final String executorName;

    private ScheduledFuture<?> future;

    public SerialBackgroundExecutor(SerialBackgroundExecutor other) {
        this(other.scheduler, other.taskRecordDao, other.taskFactory, other.pollingInterval, other.executorName);
    }

    @Override
    public void start() {
        long delayMs = Math.max(0, pollingInterval.toMillis());
        future = scheduler.scheduleWithFixedDelay(this::tick, 0, delayMs, TimeUnit.MILLISECONDS);
        log.info("{} started; polling every {}", executorName, pollingInterval);
    }

    @Override
    public void stop() {
        if (future != null) {
            future.cancel(false);
        }
        scheduler.shutdown();
        log.info("{} stopped", executorName);
    }

    @UnitOfWork
    public void tick() {
        try {
            Optional<R> next = taskRecordDao.nextTask();
            if (next.isEmpty()) {
                return;
            }
            R record = next.get();
            log.debug("{}: found next task record: {}", executorName, record);
            Runnable task = taskFactory.create(record);
            task.run();
        }
        catch (Exception e) {
            log.error("{}: error while polling or running task", executorName, e);
        }
    }
}
