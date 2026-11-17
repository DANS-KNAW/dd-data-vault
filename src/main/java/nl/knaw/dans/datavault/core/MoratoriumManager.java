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

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.config.MoratoriumConfig;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Makes the service refrain from network actions towards the remote archive for a while, to avoid triggering the hammer protection of the remote storage. One instance is shared by all
 * components that perform such actions.
 */
@Slf4j
public class MoratoriumManager {
    private final Clock clock;
    private final String triggerOn;
    private final Duration duration;
    private Instant moratoriumUntil = Instant.MIN;

    public MoratoriumManager(MoratoriumConfig config) {
        this(config.getTriggerOn(), Duration.ofMillis(config.getDuration().toMilliseconds()), Clock.systemUTC());
    }

    MoratoriumManager(String triggerOn, Duration duration, Clock clock) {
        this.triggerOn = triggerOn;
        this.duration = duration;
        this.clock = clock;
    }

    /**
     * Starts a moratorium of the configured duration. An already running moratorium is never shortened.
     */
    public synchronized void setMoratorium() {
        var newUntil = clock.instant().plus(duration);
        if (newUntil.isAfter(moratoriumUntil)) {
            moratoriumUntil = newUntil;
        }
        log.warn("Moratorium on network actions towards the remote archive set until {}", moratoriumUntil);
    }

    /**
     * Starts a moratorium if the output of a command contains the configured trigger text.
     *
     * @param output the (error) output of a command; may be null
     * @return whether a moratorium was started
     */
    public boolean checkOutput(String output) {
        if (output != null && !triggerOn.isEmpty() && output.contains(triggerOn)) {
            setMoratorium();
            return true;
        }
        return false;
    }

    public synchronized boolean isUnderMoratorium() {
        return clock.instant().isBefore(moratoriumUntil);
    }

    /**
     * Returns the time left of the moratorium, or {@link Duration#ZERO} if there is none.
     */
    public synchronized Duration remaining() {
        var left = Duration.between(clock.instant(), moratoriumUntil);
        return left.isNegative() ? Duration.ZERO : left;
    }

    /**
     * Call this before each network action.
     *
     * @throws MoratoriumException if a moratorium is in effect
     */
    public synchronized void failIfUnderMoratorium() {
        if (isUnderMoratorium()) {
            throw new MoratoriumException(moratoriumUntil);
        }
    }
}
