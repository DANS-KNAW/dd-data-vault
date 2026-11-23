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
import java.util.Arrays;
import java.util.List;

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
    private final MoratoriumManager manager = new MoratoriumManager(List.of("Connection refused"), Duration.ofMinutes(15), clock);

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
        var m = new MoratoriumManager(List.of("", ""), Duration.ofMinutes(1), clock);
        assertThat(m.checkOutput("anything")).isFalse();
        assertThat(new MoratoriumManager(List.of(), Duration.ofMinutes(1), clock).checkOutput("anything")).isFalse();
    }

    @Test
    public void any_nonempty_regex_can_match_a_substring() {
        var m = new MoratoriumManager(List.of("", "Connection refused", "(?i)connection timed? out"), Duration.ofMinutes(1), clock);
        assertThat(m.checkOutput("ssh: CONNECTION TIME OUT on port 22")).isTrue();
        assertThat(m.isUnderMoratorium()).isTrue();
        var other = new MoratoriumManager(List.of("", "Connection refused", "Connection timed out"), Duration.ofMinutes(1), clock);
        assertThat(other.checkOutput("connection refused")).isFalse();
        assertThat(other.checkOutput("Connection reset")).isFalse();
        assertThat(other.checkOutput(null)).isFalse();
        assertThat(other.checkOutput("ssh: Connection refused!")).isTrue();
    }

    @Test
    public void invalid_patterns_and_null_lists_or_elements_are_rejected() {
        assertThatThrownBy(() -> new MoratoriumManager(List.of("["), Duration.ofMinutes(1), clock))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("moratorium.triggerOn").hasMessageContaining("[");
        assertThatThrownBy(() -> new MoratoriumManager(null, Duration.ofMinutes(1), clock)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new MoratoriumManager(Arrays.asList("valid", null), Duration.ofMinutes(1), clock)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void matched_output_extends_but_never_shortens_moratorium() {
        assertThat(manager.checkOutput("Connection refused")).isTrue();
        clock.advance(Duration.ofMinutes(10));
        assertThat(manager.checkOutput("Connection refused")).isTrue();
        assertThat(manager.remaining()).isEqualTo(Duration.ofMinutes(15));
        clock.advance(Duration.ofMinutes(-5));
        manager.checkOutput("Connection refused");
        assertThat(manager.remaining()).isEqualTo(Duration.ofMinutes(20));
    }
}
