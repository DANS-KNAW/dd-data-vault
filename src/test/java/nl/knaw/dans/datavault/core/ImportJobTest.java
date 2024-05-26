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
import org.mockito.Mockito;

import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static nl.knaw.dans.lib.util.TestUtils.assertDirectoriesEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

public class ImportJobTest extends AbstractTestFixture {
    private final RepositoryProvider repositoryProvider = Mockito.mock(RepositoryProvider.class);
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    @Test
    public void run_should_process_batch_with_one_simple_object() throws Exception {
        // Given
        var simpleObject = copyToTestDir("simple-object", "batch1");
        var outbox = testDir.resolve("outbox");
        Files.createDirectories(outbox);

        // When
        var importJob = ImportJob.builder()
            .executorService(executorService)
            .repositoryProvider(repositoryProvider)
            .path(simpleObject.getParent())
            .batchOutbox(outbox)
            .build();
        importJob.run();

        // Then
        Mockito.verify(repositoryProvider).addVersion(Mockito.anyString(), eq(1), eq(simpleObject.resolve("v1")));
        assertThat(importJob.getStatus()).isEqualTo(ImportJob.Status.SUCCESS);
        assertDirectoriesEqual(getTestInput("simple-object"), outbox.resolve("processed/simple-object"));
    }

    @Test
    public void run_should_process_batch_with_two_objects() throws Exception {
        // Given
        var simpleObject = copyToTestDir("simple-object", "batch2");
        var multiVersionObject = copyToTestDir("multi-version-object", "batch2");
        var outbox = testDir.resolve("outbox");
        Files.createDirectories(outbox);

        // When
        var importJob = ImportJob.builder()
            .executorService(executorService)
            .repositoryProvider(repositoryProvider)
            .path(simpleObject.getParent())
            .batchOutbox(outbox)
            .build();
        importJob.run();

        // Then
        Mockito.verify(repositoryProvider).addVersion(Mockito.anyString(), eq(1), eq(simpleObject.resolve("v1")));
        Mockito.verify(repositoryProvider).addVersion(Mockito.anyString(), eq(1), eq(multiVersionObject.resolve("v1")));
        Mockito.verify(repositoryProvider).addVersion(Mockito.anyString(), eq(2), eq(multiVersionObject.resolve("v2")));
        assertThat(importJob.getStatus()).isEqualTo(ImportJob.Status.SUCCESS);
        assertDirectoriesEqual(getTestInput("simple-object"), outbox.resolve("processed/simple-object"));
        assertDirectoriesEqual(getTestInput("multi-version-object"), outbox.resolve("processed/multi-version-object"));
    }

    @Test
    public void run_should_reject_invalid_object_import_directory() throws Exception {
        // Given
        var invalidObject = copyToTestDir("simple-object", "batch3");
        var outbox = testDir.resolve("outbox");
        Files.createDirectories(outbox);

        // When
        var importJob = ImportJob.builder()
            .validObjectIdentifierPattern(Pattern.compile("urn:nbn:nl:ui:13-.*"))
            .executorService(executorService)
            .repositoryProvider(repositoryProvider)
            .path(invalidObject.getParent())
            .batchOutbox(outbox)
            .build();
        importJob.run();

        // Then
        assertThat(importJob.getStatus()).isEqualTo(ImportJob.Status.FAILED);
    }

    @Test
    public void run_should_make_import_job_failed_when_one_task_fails() throws Exception {
        // Given
        copyToTestDir("simple-object", "batch4");
        var multiVersionObject = copyToTestDir("multi-version-object", "batch4");
        var outbox = testDir.resolve("outbox");
        Files.createDirectories(outbox);

        // When
        // Make the second version of the multi-version-object fail
        doThrow(new RuntimeException("Failed to add version"))
            .when(repositoryProvider)
            .addVersion(eq("multi-version-object"), eq(2), eq(multiVersionObject.resolve("v2")));
        var importJob = ImportJob.builder()
            .executorService(executorService)
            .repositoryProvider(repositoryProvider)
            .path(testDir.resolve("batch4"))
            .batchOutbox(outbox)
            .build();
        importJob.run();

        // Then
        assertThat(importJob.getStatus()).isEqualTo(ImportJob.Status.FAILED);
        assertThat(importJob.getMessage()).isEqualTo(String.format("One or more tasks failed. Check error documents in '%s'.", outbox));
        assertDirectoriesEqual(getTestInput("simple-object"), outbox.resolve("processed/simple-object"));
        assertDirectoriesEqual(getTestInput("multi-version-object"), outbox.resolve("failed/multi-version-object"));
    }

}
