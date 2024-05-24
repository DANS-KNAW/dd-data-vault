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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Files;

import static nl.knaw.dans.lib.util.TestUtils.assertDirectoriesEqual;

public class ObjectCreateOrUpdateTaskTest extends AbstractTestFixture {
    private final RepositoryProvider repositoryProvider = Mockito.mock(RepositoryProvider.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        Files.createDirectories(testDir.resolve("out"));
        Files.createDirectories(testDir.resolve("out/processed"));
        Files.createDirectories(testDir.resolve("out/failed"));
    }

    @Test
    public void simple_object_should_be_added_and_moved_to_processed_folder() throws Exception {
        var simpleObject = copyToTestDir("simple-object");
        var task = new ObjectCreateOrUpdateTask(simpleObject, testDir.resolve("out"), repositoryProvider, false);
        task.run();
        Mockito.verify(repositoryProvider).addVersion(
            "simple-object",
            1,
            simpleObject.resolve("v1")
        );
        assertDirectoriesEqual(getTestInput("simple-object"), testDir.resolve("out/processed/simple-object"));
    }

    @Test
    public void multi_version_object_should_add_multiple_versions_and_be_moved_to_processed_folder() throws Exception {
        var multiVersionObject = copyToTestDir("multi-version-object");
        var task = new ObjectCreateOrUpdateTask(multiVersionObject, testDir.resolve("out"), repositoryProvider, false);
        task.run();
        Mockito.verify(repositoryProvider).addVersion(
            "multi-version-object",
            1,
            multiVersionObject.resolve("v1")
        );
        Mockito.verify(repositoryProvider).addVersion(
            "multi-version-object",
            2,
            multiVersionObject.resolve("v2")
        );
        assertDirectoriesEqual(getTestInput("multi-version-object"), testDir.resolve("out/processed/multi-version-object"));
    }

}
