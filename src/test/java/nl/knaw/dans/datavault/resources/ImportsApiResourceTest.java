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
package nl.knaw.dans.datavault.resources;

import nl.knaw.dans.datavault.api.ImportCommandDto;
import nl.knaw.dans.datavault.core.ImportJob;
import nl.knaw.dans.datavault.db.ImportJobDao;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class ImportsApiResourceTest {
    private final Path testDir = Path.of("target/test/ImportsApiResourceTest");
    private Path inbox;
    private Path outbox;
    private ImportJobDao importJobDao;
    private ImportsApiResource resource;

    @BeforeEach
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
        inbox = testDir.resolve("inbox");
        outbox = testDir.resolve("outbox");
        Files.createDirectories(inbox);
        Files.createDirectories(outbox);

        importJobDao = Mockito.mock(ImportJobDao.class);
        resource = new ImportsApiResource(importJobDao, inbox, outbox);
    }

    @Test
    public void importsCleanupPost_should_clean_inbox_and_outbox_for_all_successful_jobs() throws Exception {
        // Setup successful batch 1
        var batch1Inbox = inbox.resolve("batch1");
        var batch1Outbox = outbox.resolve("batch1");
        Files.createDirectories(batch1Inbox);
        Files.createDirectories(batch1Outbox.resolve("processed/obj1"));
        Files.writeString(batch1Outbox.resolve("processed/obj1/file.txt"), "hello");

        // Setup successful single object batch 2
        var batch2Inbox = inbox.resolve("batch2/singleObj");
        var batch2Outbox = outbox.resolve("batch2/singleObj");
        Files.createDirectories(batch2Inbox);
        Files.createDirectories(batch2Outbox.resolve("processed/singleObj"));
        Files.writeString(batch2Outbox.resolve("processed/singleObj/data.txt"), "data");

        // Setup failed batch 3
        var batch3Inbox = inbox.resolve("batch3");
        var batch3Outbox = outbox.resolve("batch3");
        Files.createDirectories(batch3Inbox);
        Files.createDirectories(batch3Outbox.resolve("failed/objFail"));
        Files.writeString(batch3Outbox.resolve("failed/objFail/error.txt"), "failed");

        var job1 = ImportJob.builder()
            .id(UUID.randomUUID())
            .path("batch1")
            .singleObject(false)
            .status(ImportJob.Status.SUCCESS)
            .created(OffsetDateTime.now().minusMinutes(10))
            .build();

        var job2 = ImportJob.builder()
            .id(UUID.randomUUID())
            .path("batch2/singleObj")
            .singleObject(true)
            .status(ImportJob.Status.SUCCESS)
            .created(OffsetDateTime.now().minusMinutes(5))
            .build();

        when(importJobDao.findByStatus(ImportJob.Status.SUCCESS)).thenReturn(List.of(job1, job2));

        // When
        var response = resource.importsCleanupPost();

        // Then
        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        assertThat(batch1Inbox).doesNotExist();
        assertThat(batch1Outbox).doesNotExist();
        assertThat(batch2Inbox).doesNotExist();
        assertThat(batch2Outbox).doesNotExist();
        assertThat(batch3Inbox).exists();
        assertThat(batch3Outbox).exists();
    }

    @Test
    public void importsCleanupPost_should_succeed_when_no_successful_jobs() {
        when(importJobDao.findByStatus(ImportJob.Status.SUCCESS)).thenReturn(List.of());

        var response = resource.importsCleanupPost();

        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    @Test
    public void importsGet_should_return_all_jobs() {
        var job = ImportJob.builder()
            .id(UUID.randomUUID())
            .path("batch1")
            .status(ImportJob.Status.PENDING)
            .created(OffsetDateTime.now())
            .build();

        when(importJobDao.list()).thenReturn(List.of(job));

        var response = resource.importsGet();

        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    @Test
    public void importsIdGet_should_return_job_when_found() {
        var id = UUID.randomUUID();
        var job = ImportJob.builder()
            .id(id)
            .path("batch1")
            .status(ImportJob.Status.PENDING)
            .created(OffsetDateTime.now())
            .build();

        when(importJobDao.get(id)).thenReturn(job);

        var response = resource.importsIdGet(id);

        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    @Test
    public void importsIdGet_should_return_404_when_not_found() {
        var id = UUID.randomUUID();
        when(importJobDao.get(id)).thenReturn(null);

        var response = resource.importsIdGet(id);

        assertThat(response.getStatus()).isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void importsPost_should_create_import_job() {
        var cmd = new ImportCommandDto();
        cmd.setPath(inbox.resolve("my-batch").toString());
        cmd.setSingleObject(false);

        var createdJob = ImportJob.builder()
            .id(UUID.randomUUID())
            .path("my-batch")
            .status(ImportJob.Status.PENDING)
            .created(OffsetDateTime.now())
            .build();

        when(importJobDao.create(any(ImportJob.class))).thenReturn(createdJob);

        var response = resource.importsPost(cmd);

        assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    @Test
    public void importsPost_should_return_bad_request_when_path_outside_inbox() {
        var cmd = new ImportCommandDto();
        cmd.setPath("/some/other/path");
        cmd.setSingleObject(false);

        var response = resource.importsPost(cmd);

        assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    }
}
