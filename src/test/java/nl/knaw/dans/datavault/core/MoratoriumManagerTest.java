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

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MoratoriumManagerTest {
    private static class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private final MutableClock clock = new MutableClock();
    private final MoratoriumManager manager = new MoratoriumManager("Connection refused", Duration.ofMinutes(15), clock);

    @Test
    public void no_moratorium_initially() {
        assertThat(manager.isUnderMoratorium()).isFalse();
        assertThat(manager.remaining()).isEqualTo(Duration.ZERO);
        assertThatCode(manager::failIfUnderMoratorium).doesNotThrowAnyException();
    }

    @Test
    public void output_with_trigger_text_starts_moratorium() {
        assertThat(manager.checkOutput("ssh: connect to host x port 22: Connection refused")).isTrue();
        assertThat(manager.isUnderMoratorium()).isTrue();
        assertThat(manager.remaining()).isEqualTo(Duration.ofMinutes(15));
        assertThatThrownBy(manager::failIfUnderMoratorium).isInstanceOf(MoratoriumException.class);
    }

    @Test
    public void output_without_trigger_text_or_null_does_not_start_moratorium() {
        assertThat(manager.checkOutput("some other error")).isFalse();
        assertThat(manager.checkOutput(null)).isFalse();
        assertThat(manager.isUnderMoratorium()).isFalse();
    }

    @Test
    public void moratorium_expires() {
        manager.setMoratorium();
        clock.advance(Duration.ofMinutes(10));
        assertThat(manager.remaining()).isEqualTo(Duration.ofMinutes(5));
        clock.advance(Duration.ofMinutes(5));
        assertThat(manager.isUnderMoratorium()).isFalse();
        assertThat(manager.remaining()).isEqualTo(Duration.ZERO);
    }

    @Test
    public void new_moratorium_extends_but_never_shortens() {
        manager.setMoratorium();
        clock.advance(Duration.ofMinutes(10));
        manager.setMoratorium();
        assertThat(manager.remaining()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    public void empty_trigger_text_never_triggers() {
        var m = new MoratoriumManager("", Duration.ofMinutes(1), clock);
        assertThat(m.checkOutput("anything")).isFalse();
    }
}
