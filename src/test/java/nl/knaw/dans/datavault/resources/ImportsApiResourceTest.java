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
import nl.knaw.dans.datavault.api.RestageFailedRequestDto;
import nl.knaw.dans.datavault.core.ImportJob;
import nl.knaw.dans.datavault.db.ImportJobDao;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

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

        when(importJobDao.listFiltered(EnumSet.noneOf(ImportJob.Status.class), null, null, null)).thenReturn(List.of(job));

        var response = resource.importsGet(null, null, null, null);

        assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
        assertThat((List<?>) response.getEntity()).hasSize(1);
    }

    @Test
    public void imports_get_normalizes_batch_and_passes_filters() {
        var statuses = EnumSet.of(ImportJob.Status.PENDING, ImportJob.Status.FAILED);
        assertThat(resource.importsGet(List.of("PENDING", "FAILED"), 10, 2, "dir/../batch").getStatus()).isEqualTo(200);
        verify(importJobDao).listFiltered(statuses, "batch", 10, 2);
        assertThat(resource.importsGet(null, null, null, inbox.toAbsolutePath().resolve("dir/../batch").toString()).getStatus()).isEqualTo(200);
        verify(importJobDao).listFiltered(EnumSet.noneOf(ImportJob.Status.class), "batch", null, null);
    }

    @Test
    public void imports_get_rejects_invalid_filters() {
        assertThat(resource.importsGet(List.of("INVALID"), null, null, null).getStatus()).isEqualTo(400);
        assertThat(resource.importsGet(null, 0, null, null).getStatus()).isEqualTo(400);
        assertThat(resource.importsGet(null, 1, -1, null).getStatus()).isEqualTo(400);
        assertThat(resource.importsGet(null, null, 0, null).getStatus()).isEqualTo(400);
        assertThat(resource.importsGet(null, null, null, "../escape").getStatus()).isEqualTo(400);
        assertThat(resource.importsGet(null, null, null, inbox.toAbsolutePath().resolveSibling("inbox-other/batch").toString()).getStatus()).isEqualTo(400);
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

    @Test
    public void restage_moves_failed_objects_and_removes_only_their_error_files() throws Exception {
        var failed = outbox.resolve("batch/failed");
        Files.createDirectories(failed.resolve("object/v1"));
        Files.writeString(failed.resolve("object-error.txt"), "error");
        Files.writeString(failed.resolve("other-error.txt"), "keep");
        assertThat(restage("batch").getStatus()).isEqualTo(200);
        assertThat(inbox.resolve("batch/object/v1")).isDirectory();
        assertThat(failed.resolve("object")).doesNotExist();
        assertThat(failed.resolve("object-error.txt")).doesNotExist();
        assertThat(failed.resolve("other-error.txt")).exists();
        assertThat(restage("batch").getStatus()).isEqualTo(200);
    }

    @Test
    public void restage_single_object_by_job_id() throws Exception {
        var id = UUID.randomUUID();
        when(importJobDao.get(id)).thenReturn(ImportJob.builder().path("batch/object").singleObject(true).build());
        var failed = outbox.resolve("batch/object/failed");
        Files.createDirectories(failed.resolve("object/v1"));
        Files.createDirectories(failed.resolve("other/v1"));
        Files.writeString(failed.resolve("object-error.txt"), "remove");
        Files.writeString(failed.resolve("other-error.txt"), "keep");
        Files.createSymbolicLink(failed.resolve("unrelated-link"), testDir.toAbsolutePath());
        assertThat(restage(id.toString()).getStatus()).isEqualTo(200);
        assertThat(inbox.resolve("batch/object/v1")).isDirectory();
        assertThat(failed.resolve("object")).doesNotExist();
        assertThat(failed.resolve("object-error.txt")).doesNotExist();
        assertThat(failed.resolve("other/v1")).isDirectory();
        assertThat(failed.resolve("other-error.txt")).hasContent("keep");
        assertThat(Files.isSymbolicLink(failed.resolve("unrelated-link"))).isTrue();
    }

    @Test
    public void restage_single_object_rejects_selected_source_symlink() throws Exception {
        var id = UUID.randomUUID();
        when(importJobDao.get(id)).thenReturn(ImportJob.builder().path("batch/object").singleObject(true).build());
        var failed = outbox.resolve("batch/object/failed");
        Files.createDirectories(failed);
        Files.createSymbolicLink(failed.resolve("object"), inbox.toAbsolutePath());

        assertThat(restage(id.toString()).getStatus()).isEqualTo(400);
        assertThat(Files.isSymbolicLink(failed.resolve("object"))).isTrue();
        assertThat(inbox.resolve("batch/object")).doesNotExist();
    }

    @Test
    public void restage_uuid_shaped_path_and_missing_failed_directory() throws Exception {
        var name = UUID.randomUUID().toString();
        Files.createDirectories(outbox.resolve(name).resolve("failed/object"));
        assertThat(restage(name).getStatus()).isEqualTo(404);
        assertThat(restage("./" + name).getStatus()).isEqualTo(200);
        assertThat(inbox.resolve(name).resolve("object")).isDirectory();
        assertThat(restage("absent").getStatus()).isEqualTo(200);
    }

    @Test
    public void restage_rejects_conflicts_before_moving_anything() throws Exception {
        var failed = outbox.resolve("batch/failed");
        Files.createDirectories(failed.resolve("a"));
        Files.createDirectories(failed.resolve("b"));
        Files.createDirectories(inbox.resolve("batch/b"));
        assertThat(restage("batch").getStatus()).isEqualTo(409);
        assertThat(failed.resolve("a")).exists();
        assertThat(failed.resolve("b")).exists();
    }

    @Test
    public void restage_error_message_with_user_provided_path_is_plain_text() throws Exception {
        var name = "<img src=x onerror=alert(1)>";
        Files.createDirectories(outbox.resolve("batch/failed").resolve(name));
        Files.createDirectories(inbox.resolve("batch").resolve(name));

        var response = restage("batch");

        assertThat(response.getStatus()).isEqualTo(409);
        assertThat(response.getMediaType()).isEqualTo(MediaType.TEXT_PLAIN_TYPE);
        assertThat((String) response.getEntity()).startsWith("Destination already exists: ").endsWith(name);
    }

    @Test
    public void restage_rejects_active_overlapping_jobs_and_invalid_paths() throws Exception {
        when(importJobDao.listFiltered(EnumSet.of(ImportJob.Status.PENDING, ImportJob.Status.RUNNING), null, null, null))
            .thenReturn(List.of(ImportJob.builder().path("batch/object").build()));
        assertThat(restage("batch").getStatus()).isEqualTo(409);
        assertThat(restage("../outside").getStatus()).isEqualTo(400);
        assertThat(restage("").getStatus()).isEqualTo(400);
        assertThat(restage(UUID.randomUUID().toString()).getStatus()).isEqualTo(404);
        Files.createSymbolicLink(outbox.resolve("link"), testDir.toAbsolutePath());
        assertThat(restage("link").getStatus()).isEqualTo(400);
    }

    private Response restage(String selector) {
        var request = new RestageFailedRequestDto();
        request.setSelector(selector);
        return resource.importsRestageFailedPost(request);
    }
}
