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
import nl.knaw.dans.datavault.db.TaskSource;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 * @param <R> record type
 */
@Slf4j
@RequiredArgsConstructor
public class PollingTaskExecutor<R> implements Managed {
    private final String name;
    private final ScheduledExecutorService scheduler;
    private final Duration pollingInterval;
    private final TaskSource<R> taskSource;
    private final TaskFactory<R> taskFactory;

    private ScheduledFuture<?> future;

    /**
     * Copy constructor. The source executor must not be running.
     *
     * @param other the source executor
     */
    @SuppressWarnings("CopyConstructorMissesField") // future is not copied on purpose
    public PollingTaskExecutor(PollingTaskExecutor<R> other) {
        if (other.future != null) {
            throw new IllegalArgumentException("Cannot copy a running executor");
        }
        this.name = other.name;
        this.scheduler = other.scheduler;
        this.pollingInterval = other.pollingInterval;
        this.taskSource = other.taskSource;
        this.taskFactory = other.taskFactory;
    }

    @Override
    public void start() {
        long delayMs = Math.max(0, pollingInterval.toMillis());
        future = scheduler.scheduleWithFixedDelay(this::tick, 0, delayMs, TimeUnit.MILLISECONDS);
        log.info("{} started; polling every {}", name, pollingInterval);
    }

    @Override
    public void stop() {
        if (future != null) {
            future.cancel(false);
        }
        scheduler.shutdown();
        log.info("{} stopped", name);
    }

    @UnitOfWork
    public void tick() {
        try {
            Optional<R> next = taskSource.nextTask();
            if (next.isEmpty()) {
                return;
            }
            R record = next.get();
            log.debug("{}: found next task record: {}", name, record);
            Runnable task = taskFactory.create(record);
            task.run();
        }
        catch (Exception e) {
            log.error("{}: error while polling or running task", name, e);
        }
    }
}
