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

import nl.knaw.dans.datavault.api.CopyDirectoryIntoRequestDto;
import nl.knaw.dans.datavault.api.CopyFileIntoRequestDto;
import nl.knaw.dans.datavault.api.CopyFileOutOfRequestDto;
import nl.knaw.dans.datavault.config.ItemstoreConfig;
import nl.knaw.dans.datavault.config.ItemstoreEndpointsConfig;
import nl.knaw.dans.layerstore.ItemStore;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ItemstoreApiResourceTest {
    private ItemStore layeredItemStore;
    private ItemstoreConfig itemstoreConfig;
    private ItemstoreApiResource resource;
    private final Path testDir = Path.of("target/test/ItemstoreApiResourceTest").toAbsolutePath();
    private final Path exchange = testDir.resolve("exchange");
    private final Path outside = testDir.resolve("outside");

    @BeforeEach
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
        Files.createDirectories(exchange);
        Files.createDirectories(outside);
        layeredItemStore = Mockito.mock(ItemStore.class);
        itemstoreConfig = new ItemstoreConfig();
        var endpoints = new ItemstoreEndpointsConfig();
        endpoints.setCopyDirectoryInto(true);
        endpoints.setCopyFileOutOf(true);
        endpoints.setCopyFileInto(true);
        itemstoreConfig.setEnableEndpoints(endpoints);
        itemstoreConfig.setWorkDir(testDir.resolve("work").toString());
        itemstoreConfig.setExchangeDir(exchange);
        resource = new ItemstoreApiResource(layeredItemStore, itemstoreConfig);
    }

    @Test
    public void itemstoreCopyDirectoryIntoPost_should_return_bad_request_when_source_is_not_absolute() {
        var request = new CopyDirectoryIntoRequestDto();
        request.setSource("relative/path");
        request.setDestination("dest");

        var response = resource.itemstoreCopyDirectoryIntoPost(request);

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals("Source path must be absolute", response.getEntity());
    }

    @Test
    public void itemstoreCopyFileIntoPost_should_return_bad_request_when_source_is_not_absolute() {
        var request = new CopyFileIntoRequestDto();
        request.setSource("relative/path.txt");
        request.setDestination("dest.txt");

        var response = resource.itemstoreCopyFileIntoPost(request);

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals("Source path must be absolute", response.getEntity());
    }

    @Test
    public void itemstoreCopyFileOutOfPost_should_return_bad_request_when_destination_is_not_absolute() {
        var request = new CopyFileOutOfRequestDto();
        request.setSource("source.txt");
        request.setDestination("relative/dest.txt");

        var response = resource.itemstoreCopyFileOutOfPost(request);

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals("Destination path must be absolute", response.getEntity());
    }

    private Response copyFileInto(Path source) {
        var request = new CopyFileIntoRequestDto();
        request.setSource(source.toString());
        request.setDestination("dest.txt");
        return resource.itemstoreCopyFileIntoPost(request);
    }

    private Response copyFileOutOf(Path destination) {
        var request = new CopyFileOutOfRequestDto();
        request.setSource("source.txt");
        request.setDestination(destination.toString());
        return resource.itemstoreCopyFileOutOfPost(request);
    }

    private Response copyDirectoryInto(Path source) {
        var request = new CopyDirectoryIntoRequestDto();
        request.setSource(source.toString());
        request.setDestination("dest");
        return resource.itemstoreCopyDirectoryIntoPost(request);
    }

    private void assertOutsideExchangeDir(Response response) {
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getMediaType()).isEqualTo(MediaType.TEXT_PLAIN_TYPE);
        assertThat((String) response.getEntity()).endsWith("path must be inside the exchange directory (dataVault.itemstore.exchangeDir)");
    }

    @Test
    public void copy_file_into_reads_file_from_exchange_dir() throws Exception {
        Files.writeString(exchange.resolve("file.txt"), "content");

        assertThat(copyFileInto(exchange.resolve("file.txt")).getStatus()).isEqualTo(200);

        verify(layeredItemStore).writeFile(eq("dest.txt"), any());
    }

    @Test
    public void copy_file_into_refuses_file_outside_exchange_dir_including_via_dot_dot_and_symlink() throws Exception {
        var secret = Files.writeString(outside.resolve("secret.txt"), "secret");
        Files.createSymbolicLink(exchange.resolve("link.txt"), secret);

        assertOutsideExchangeDir(copyFileInto(secret));
        assertOutsideExchangeDir(copyFileInto(exchange.resolve("../outside/secret.txt")));
        assertOutsideExchangeDir(copyFileInto(exchange.resolve("link.txt")));

        verify(layeredItemStore, never()).writeFile(anyString(), any());
    }

    @Test
    public void copy_file_into_does_not_reveal_whether_file_outside_exchange_dir_exists() {
        assertOutsideExchangeDir(copyFileInto(outside.resolve("does-not-exist.txt")));
    }

    @Test
    public void copy_file_out_of_writes_new_file_in_exchange_dir() throws Exception {
        when(layeredItemStore.readFile("source.txt")).thenReturn(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        assertThat(copyFileOutOf(exchange.resolve("out.txt")).getStatus()).isEqualTo(200);

        assertThat(exchange.resolve("out.txt")).hasContent("content");
    }

    @Test
    public void copy_file_out_of_refuses_destination_outside_exchange_dir_including_via_dot_dot_and_symlinked_parent() throws Exception {
        Files.createSymbolicLink(exchange.resolve("linked-dir"), outside);

        assertOutsideExchangeDir(copyFileOutOf(outside.resolve("out.txt")));
        assertOutsideExchangeDir(copyFileOutOf(exchange.resolve("../outside/out.txt")));
        assertOutsideExchangeDir(copyFileOutOf(exchange.resolve("linked-dir/out.txt")));

        assertThat(outside).isEmptyDirectory();
        verify(layeredItemStore, never()).readFile(anyString());
    }

    @Test
    public void copy_file_out_of_refuses_existing_destination() throws Exception {
        Files.writeString(exchange.resolve("out.txt"), "original");

        var response = copyFileOutOf(exchange.resolve("out.txt"));

        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getEntity()).isEqualTo("Destination already exists");
        assertThat(exchange.resolve("out.txt")).hasContent("original");
    }

    @Test
    public void copy_directory_into_reads_directory_from_exchange_dir() throws Exception {
        Files.createDirectories(exchange.resolve("dir/sub"));
        Files.writeString(exchange.resolve("dir/sub/file.txt"), "content");

        assertThat(copyDirectoryInto(exchange.resolve("dir")).getStatus()).isEqualTo(200);

        verify(layeredItemStore).moveDirectoryInto(any(), eq("dest"));
    }

    @Test
    public void copy_directory_into_refuses_directory_outside_exchange_dir_or_containing_symlinks() throws Exception {
        Files.createDirectories(exchange.resolve("dir"));
        Files.createSymbolicLink(exchange.resolve("dir/link"), outside);

        assertOutsideExchangeDir(copyDirectoryInto(outside));
        var response = copyDirectoryInto(exchange.resolve("dir"));
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getEntity()).isEqualTo("Source directory contains symbolic links");

        verify(layeredItemStore, never()).moveDirectoryInto(any(), anyString());
    }
}
