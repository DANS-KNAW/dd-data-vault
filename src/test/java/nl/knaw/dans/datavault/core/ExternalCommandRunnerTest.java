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

import io.dropwizard.util.Duration;
import nl.knaw.dans.datavault.config.ExternalCommandConfig;
import nl.knaw.dans.datavault.config.MoratoriumConfig;
import nl.knaw.dans.datavault.config.RetryConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExternalCommandRunnerTest {
    private final MoratoriumConfig moratoriumConfig = new MoratoriumConfig();
    private final MoratoriumManager moratoriumManager = new MoratoriumManager(moratoriumConfig);
    private final RetryConfig retryConfig = new RetryConfig();
    private final ExternalCommandRunner runner = new ExternalCommandRunner(moratoriumManager, retryConfig);

    public ExternalCommandRunnerTest() {
        retryConfig.setMaxAttempts(3);
        retryConfig.setBackoff(Duration.milliseconds(1));
    }

    @Test
    public void run_substitutes_placeholders_and_captures_stdout_and_stderr() {
        var command = new ExternalCommandConfig("/bin/sh", List.of("-c", "echo out-$0; echo err-$1 >&2", "${a}", "${b}"));

        var result = runner.run(command, Map.of("a", "A", "b", "B"));

        assertThat(result.succeeded()).isTrue();
        assertThat(result.output()).contains("out-A").contains("err-B");
    }

    @Test
    public void run_returns_non_zero_exit_code_without_throwing() {
        var result = runner.run(new ExternalCommandConfig("/bin/sh", List.of("-c", "exit 3")), Map.of());

        assertThat(result.exitCode()).isEqualTo(3);
        assertThat(result.succeeded()).isFalse();
    }

    @Test
    public void run_starts_moratorium_when_failing_command_outputs_trigger_text() {
        runner.run(new ExternalCommandConfig("/bin/sh", List.of("-c", "echo 'Connection refused'; exit 255")), Map.of());

        assertThat(moratoriumManager.isUnderMoratorium()).isTrue();
        assertThatThrownBy(() -> runner.run(new ExternalCommandConfig("/bin/true", List.of()), Map.of())).isInstanceOf(MoratoriumException.class);
    }

    @Test
    public void runWithRetry_throws_when_all_attempts_fail() {
        assertThatThrownBy(() -> runner.runWithRetry(new ExternalCommandConfig("/bin/sh", List.of("-c", "echo nope; exit 1")), Map.of(), "Doing it", null))
            .isInstanceOf(CommandFailedException.class)
            .hasMessageContaining("Doing it")
            .hasMessageContaining("nope");
    }

    @Test
    public void runWithRetry_returns_failed_result_without_retrying_when_output_matches_stopOnOutput() {
        var result = runner.runWithRetry(new ExternalCommandConfig("/bin/sh", List.of("-c", "echo 'checksum failure'; exit 1")), Map.of(), "Verifying", "checksum failure");

        assertThat(result.succeeded()).isFalse();
    }

    @Test
    public void runWithRetry_throws_MoratoriumException_instead_of_retrying_when_moratorium_starts() {
        assertThatThrownBy(() -> runner.runWithRetry(new ExternalCommandConfig("/bin/sh", List.of("-c", "echo 'Connection refused'; exit 255")), Map.of(), "Uploading", null))
            .isInstanceOf(MoratoriumException.class);
    }
}
