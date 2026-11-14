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
import nl.knaw.dans.datavault.config.ExternalCommandConfig;
import nl.knaw.dans.datavault.config.RetryConfig;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Runs configured external commands towards the remote archive. All commands are subject to the moratorium: none is started while a moratorium is in effect, and a failing command whose
 * output contains the moratorium trigger text starts one.
 */
@Slf4j
public class ExternalCommandRunner {
    public record CommandResult(int exitCode, String output) {
        public boolean succeeded() {
            return exitCode == 0;
        }
    }

    private final MoratoriumManager moratoriumManager;
    private final RetryConfig retryConfig;

    public ExternalCommandRunner(MoratoriumManager moratoriumManager, RetryConfig retryConfig) {
        this.moratoriumManager = moratoriumManager;
        this.retryConfig = retryConfig;
    }

    /**
     * Runs the command once. The arguments are passed as they are (no shell is involved), after replacing the placeholders, e.g. <code>${name}</code>.
     *
     * @return the exit code and the combined standard output and standard error of the command
     * @throws MoratoriumException if a moratorium is in effect
     */
    public CommandResult run(ExternalCommandConfig command, Map<String, String> placeholders) {
        moratoriumManager.failIfUnderMoratorium();
        var commandLine = new CommandLine(substitute(command.getExecutable(), placeholders));
        command.getArgs().forEach(arg -> commandLine.addArgument(substitute(arg, placeholders), false));
        log.debug("Executing command: {}", commandLine);

        var outputStream = new ByteArrayOutputStream();
        var executor = DefaultExecutor.builder().get();
        executor.setExitValues(null);
        executor.setStreamHandler(new PumpStreamHandler(outputStream));
        try {
            var exitCode = executor.execute(commandLine);
            var output = outputStream.toString(StandardCharsets.UTF_8);
            if (exitCode != 0) {
                moratoriumManager.checkOutput(output);
            }
            return new CommandResult(exitCode, output);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not run command " + commandLine, e);
        }
    }

    /**
     * Runs the command until it succeeds, waiting the configured backoff time between attempts.
     *
     * @param description a description of the action, for logging and error messages
     * @param stopOnOutput if the output of a failed attempt contains this text, the failed result is returned instead of trying again; may be null
     * @return the result of the successful attempt, or of the attempt that stopped the retrying
     * @throws MoratoriumException    if a moratorium is (or becomes) in effect
     * @throws CommandFailedException if the maximum number of attempts is used up
     */
    public CommandResult runWithRetry(ExternalCommandConfig command, Map<String, String> placeholders, String description, String stopOnOutput) {
        for (var attempt = 1; ; attempt++) {
            var result = run(command, placeholders);
            if (result.succeeded() || (stopOnOutput != null && result.output().contains(stopOnOutput))) {
                return result;
            }
            log.warn("{} failed (attempt {} of {}), exit code {}: {}", description, attempt, retryConfig.getMaxAttempts(), result.exitCode(), result.output());
            moratoriumManager.failIfUnderMoratorium();
            if (attempt >= retryConfig.getMaxAttempts()) {
                throw new CommandFailedException(description, result.exitCode(), result.output());
            }
            try {
                Thread.sleep(retryConfig.getBackoff().toMilliseconds());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to retry: " + description, e);
            }
        }
    }

    private static String substitute(String template, Map<String, String> placeholders) {
        var result = template;
        for (var entry : placeholders.entrySet()) {
            result = result.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return result;
    }
}
