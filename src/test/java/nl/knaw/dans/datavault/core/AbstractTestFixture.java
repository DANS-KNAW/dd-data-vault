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

import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractTestFixture {
    protected final Path testDir = Path.of("target/test")
        .resolve(getClass().getSimpleName());

    @BeforeEach
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(testDir.toFile());
        Files.createDirectories(testDir);
    }

    protected Path getTestInput(String name) {
        return Path.of("src/test/resources/test-input").resolve(name);
    }

    protected Path copyToTestDir(String source) throws Exception {
        var sourcePath = getTestInput(source);
        Path target = testDir.resolve(sourcePath.getFileName());
        FileUtils.copyDirectory(sourcePath.toFile(), target.toFile());
        return target;
    }

}
