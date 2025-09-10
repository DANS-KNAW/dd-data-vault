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

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages scheduling and execution of a single layer-ids check at a time.
 */
@Slf4j
@RequiredArgsConstructor
public class LayerIdsCheckManager {

    public enum Status {
        PENDING, RUNNING, OK, FAILED, ERROR
    }

    private final ExecutorService executor;
    private final LayerIdsCheck checkTask; // your domain service that performs the check

    private final Map<UUID, Status> statuses = new ConcurrentHashMap<>();
    private final AtomicReference<UUID> activeOrPending = new AtomicReference<>(null);
    private static final Set<Status> NON_TERMINAL = EnumSet.of(Status.PENDING, Status.RUNNING);

    public LayerIdsCheckManager(LayerIdsCheckManager other) {
        this.executor = other.executor;
        this.checkTask = other.checkTask;
        this.statuses.putAll(other.statuses);
        this.activeOrPending.set(other.activeOrPending.get());
    }

    /**
     * @return true if a check is currently pending or running.
     */
    public boolean hasActiveOrPending() {
        UUID id = activeOrPending.get();
        if (id == null) {
            return false;
        }
        Status s = statuses.get(id);
        return s != null && NON_TERMINAL.contains(s);
    }

    /**
     * Attempts to schedule a new check. Returns empty if another check is pending/running.
     */
    public Optional<UUID> trySchedule() {
        if (!activeOrPending.compareAndSet(null, new UUID(0L, 0L))) {
            // someone already set a marker; double-check the state
            if (hasActiveOrPending()) {
                return Optional.empty();
            }
            // clear stale marker if needed
            activeOrPending.set(null);
        }

        try {
            UUID id = UUID.randomUUID();
            statuses.put(id, Status.PENDING);
            activeOrPending.set(id);

            executor.submit(() -> {
                statuses.put(id, Status.RUNNING);
                try {
                    checkTask.check(); // perform the actual check
                    statuses.put(id, Status.OK);
                }
                catch(IllegalStateException e) {
                    statuses.put(id, Status.ERROR);
                }
                catch (Throwable t) {
                    log.error("Layer IDs check failed for {}", id, t);
                    statuses.put(id, Status.FAILED);
                }
                finally {
                    // allow a new schedule
                    activeOrPending.compareAndSet(id, null);
                }
            });

            return Optional.of(id);
        }
        catch (Throwable t) {
            // in case submission fails
            activeOrPending.set(null);
            throw t;
        }
    }

    public Optional<Status> getStatus(UUID id) {
        return Optional.ofNullable(statuses.get(id));
    }
}
