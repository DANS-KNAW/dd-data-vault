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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class BatchCleanerTest {
    private final Path testDir = Path.of("target/test/BatchCleanerTest");

    @BeforeEach
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
        Files.createDirectories(testDir);
    }

    @Test
    public void cleanProcessedObject_should_delete_object_from_processed_directory() throws Exception {
        var outbox = testDir.resolve("outbox");
        var processedDir = outbox.resolve("processed");
        var obj1Dir = processedDir.resolve("obj1");
        Files.createDirectories(obj1Dir);
        Files.writeString(obj1Dir.resolve("data.txt"), "test data");

        var cleaner = new BatchCleaner(testDir.resolve("inbox"), outbox);
        cleaner.cleanProcessedObject(Path.of("obj1"));

        assertThat(obj1Dir).doesNotExist();
    }

    @Test
    public void cleanProcessedObject_by_string_should_delete_object_from_processed_directory() throws Exception {
        var outbox = testDir.resolve("outbox");
        var processedDir = outbox.resolve("processed");
        var obj1Dir = processedDir.resolve("obj1");
        Files.createDirectories(obj1Dir);
        Files.writeString(obj1Dir.resolve("data.txt"), "test data");

        var cleaner = new BatchCleaner(outbox);
        cleaner.cleanProcessedObject("obj1");

        assertThat(obj1Dir).doesNotExist();
    }

    @Test
    public void cleanProcessedObjects_with_list_should_delete_specified_objects() throws Exception {
        var outbox = testDir.resolve("outbox");
        var processedDir = outbox.resolve("processed");
        var obj1Dir = processedDir.resolve("obj1");
        var obj2Dir = processedDir.resolve("obj2");
        var obj3Dir = processedDir.resolve("obj3");
        Files.createDirectories(obj1Dir);
        Files.createDirectories(obj2Dir);
        Files.createDirectories(obj3Dir);

        var cleaner = new BatchCleaner(outbox);
        cleaner.cleanProcessedObjects(List.of(Path.of("path/to/obj1"), Path.of("path/to/obj2")));

        assertThat(obj1Dir).doesNotExist();
        assertThat(obj2Dir).doesNotExist();
        assertThat(obj3Dir).exists();
    }

    @Test
    public void cleanProcessedObjects_no_args_should_delete_all_objects_in_processed_directory() throws Exception {
        var outbox = testDir.resolve("outbox");
        var processedDir = outbox.resolve("processed");
        var obj1Dir = processedDir.resolve("obj1");
        var obj2Dir = processedDir.resolve("obj2");
        Files.createDirectories(obj1Dir);
        Files.createDirectories(obj2Dir);

        var cleaner = new BatchCleaner(outbox);
        cleaner.cleanProcessedObjects();

        assertThat(obj1Dir).doesNotExist();
        assertThat(obj2Dir).doesNotExist();
    }

    @Test
    public void cleanSuccessfulProcessedObjects_should_delete_only_successful_tasks() throws Exception {
        var outbox = testDir.resolve("outbox");
        var processedDir = outbox.resolve("processed");
        var obj1Dir = processedDir.resolve("obj1");
        var obj2Dir = processedDir.resolve("obj2");
        Files.createDirectories(obj1Dir);
        Files.createDirectories(obj2Dir);

        var task1 = Mockito.mock(ObjectCreateOrUpdateTask.class);
        Mockito.when(task1.getStatus()).thenReturn(ObjectCreateOrUpdateTask.Status.SUCCESS);

        var task2 = Mockito.mock(ObjectCreateOrUpdateTask.class);
        Mockito.when(task2.getStatus()).thenReturn(ObjectCreateOrUpdateTask.Status.FAILED);

        var cleaner = new BatchCleaner(outbox);
        cleaner.cleanSuccessfulProcessedObjects(List.of(task1, task2), List.of(Path.of("obj1"), Path.of("obj2")));

        assertThat(obj1Dir).doesNotExist();
        assertThat(obj2Dir).exists();
    }

    @Test
    public void deleteBatchDirs_should_delete_inbox_and_outbox_directories() throws Exception {
        var inboxDir = testDir.resolve("inbox/batch1");
        var outboxDir = testDir.resolve("outbox/batch1");
        Files.createDirectories(inboxDir);
        Files.createDirectories(outboxDir);

        var cleaner = new BatchCleaner(inboxDir, outboxDir);
        cleaner.deleteBatchDirs();

        assertThat(inboxDir).doesNotExist();
        assertThat(outboxDir).doesNotExist();
    }

    @Test
    public void deleteBatchDirsIfSucceeded_should_delegate_to_deleteBatchDirs() throws Exception {
        var inboxDir = testDir.resolve("inbox/batch2");
        var outboxDir = testDir.resolve("outbox/batch2");
        Files.createDirectories(inboxDir);
        Files.createDirectories(outboxDir);

        var cleaner = new BatchCleaner(inboxDir, outboxDir);
        cleaner.deleteBatchDirsIfSucceeded();

        assertThat(inboxDir).doesNotExist();
        assertThat(outboxDir).doesNotExist();
    }

    @Test
    public void methods_should_handle_missing_directories_and_nulls_gracefully() {
        var cleanerWithNulls = new BatchCleaner(null, null);

        assertThatCode(() -> {
            cleanerWithNulls.cleanProcessedObject("nonexistent");
            cleanerWithNulls.cleanProcessedObject(Path.of("nonexistent"));
            cleanerWithNulls.cleanProcessedObjects(List.of(Path.of("nonexistent")));
            cleanerWithNulls.cleanProcessedObjects();
            cleanerWithNulls.deleteBatchDirs();
        }).doesNotThrowAnyException();
    }
}
