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
import nl.knaw.dans.datavault.config.DmfTarArchiveProviderFactory;
import nl.knaw.dans.layerstore.Archive;
import nl.knaw.dans.layerstore.Item;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * An {@link Archive} that is a dmftar directory on a remote host, which is not written to the remote host directly by dmftar, but created locally first and then transferred with rsync, so that
 * interrupted connections can be resumed.
 * <p>
 * Archiving a layer takes these steps, each of which can be repeated after a failure or a restart of the service:
 * <ol>
 *     <li>create the dmftar directory in the local transfer root (a leftover complete one from an earlier attempt is reused);</li>
 *     <li>rsync it into the directory <code>&lt;name&gt;.partial</code> under the remote archive root, retrying until it is complete;</li>
 *     <li>run dmftar's verify on the remote copy;</li>
 *     <li>move the verified copy to its final place in the remote archive root, replacing an existing one;</li>
 *     <li>remove the local dmftar directory.</li>
 * </ol>
 * The dmftar directory itself cannot be renamed after the transfer, because the names of the volumes inside it are derived from its name; therefore the <code>.partial</code>
 * directory is a wrapper around it. The archive only becomes visible in the remote archive root at step 4, so a partial transfer is never mistaken for an archive. The caller removes the staging directory after
 * {@link #archiveFrom(Path)} returns normally.
 * <p>
 * Reading from the archive is delegated to the given remote archive.
 */
@Slf4j
public class RsyncedDmfTarArchive implements Archive {
    public static final String PARTIAL_SUFFIX = ".partial";
    private static final String COMPLETE_MARKER_SUFFIX = ".complete";
    private static final Set<PosixFilePermission> DELETE_PERMISSIONS = PosixFilePermissions.fromString("rwxr-xr-x");

    private final Archive remoteArchive;
    private final MoratoriumManager moratoriumManager;
    private final ExternalCommandRunner commandRunner;
    private final LocalDmfTarRunner localDmfTarRunner;
    private final Semaphore unarchiveSlots;
    private final DmfTarArchiveProviderFactory config;
    private final String name;
    private final Path remoteDir;
    private final Path remoteTransferDir;
    private final Path localDmfTar;
    private final Path completeMarker;
    private final Map<String, String> placeholders;

    public RsyncedDmfTarArchive(long layerId, Archive remoteArchive, DmfTarArchiveProviderFactory config, ExternalCommandRunner commandRunner,
        LocalDmfTarRunner localDmfTarRunner, MoratoriumManager moratoriumManager, Semaphore unarchiveSlots) {
        this.remoteArchive = remoteArchive;
        this.config = config;
        this.commandRunner = commandRunner;
        this.localDmfTarRunner = localDmfTarRunner;
        this.moratoriumManager = moratoriumManager;
        this.unarchiveSlots = unarchiveSlots;
        this.name = layerId + ".dmftar";
        this.remoteDir = config.getArchiveRoot();
        this.remoteTransferDir = remoteDir.resolve(name + PARTIAL_SUFFIX);
        var localTransferRoot = config.getLocalTransferRoot().toAbsolutePath();
        this.localDmfTar = localTransferRoot.resolve(name);
        this.completeMarker = localTransferRoot.resolve(name + COMPLETE_MARKER_SUFFIX);
        this.placeholders = Map.of(
            "user", config.getUser(),
            "host", config.getHost(),
            "remoteDir", remoteDir.toString(),
            "remoteTransferDir", remoteTransferDir.toString(),
            "localDir", localTransferRoot.toString(),
            "name", name);
    }

    @Override
    public InputStream readFile(String filePath) throws IOException {
        moratoriumManager.failIfUnderMoratorium();
        return remoteArchive.readFile(filePath);
    }

    /**
     * Downloads the dmftar directory with rsync (retrying and resuming as needed), optionally verifies it, unpacks it into the staging directory and removes the local copy. At most the configured
     * number of layers is unarchived at the same time.
     */
    @Override
    public void unarchiveTo(Path stagingDir) {
        moratoriumManager.failIfUnderMoratorium();
        try {
            unarchiveSlots.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to unarchive " + name, e);
        }
        try {
            if (Files.exists(completeMarker)) {
                // Leftover of archiving the layer; it may differ from what is in the archive
                deleteLocalDmfTar();
            }
            Files.createDirectories(localDmfTar.getParent());
            commandRunner.runWithRetry(config.getRsyncDownload(), placeholders, "Downloading " + name, null);
            if (config.isVerifyDownloadedCopy() && !localDmfTarRunner.verify(localDmfTar)) {
                // Download again next time
                deleteLocalDmfTar();
                throw new IllegalStateException("Verification of downloaded " + name + " failed");
            }
            localDmfTarRunner.untarArchive(localDmfTar, stagingDir);
            deleteLocalDmfTar();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not unarchive " + name + " to " + stagingDir, e);
        }
        finally {
            unarchiveSlots.release();
        }
    }

    @Override
    public void archiveFrom(Path stagingDir) {
        try {
            prepareLocalDmfTar(stagingDir);
            commandRunner.runWithRetry(config.getRsyncUpload(), placeholders, "Uploading " + name, null);
            verifyRemoteCopy();
            moveRemoteCopyIntoPlace();
            deleteLocalDmfTar();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not archive " + stagingDir + " as " + name, e);
        }
    }

    private void prepareLocalDmfTar(Path stagingDir) throws IOException {
        if (isCachedTarValid(stagingDir)) {
            log.info("Reusing complete local dmftar {} from an earlier attempt", localDmfTar);
            return;
        }
        deleteLocalDmfTar();
        // A copy from an earlier attempt may be based on different content, which rsync does not clean up
        runRemote("if [ -e %1$s ]; then chmod -R u+w %1$s 2>/dev/null || true; rm -rf %1$s; fi".formatted(quote(remoteTransferDir)), "Removing stale upload of " + name);
        Files.createDirectories(localDmfTar.getParent());
        log.info("Creating local dmftar {}", localDmfTar);
        localDmfTarRunner.tarDirectory(stagingDir, localDmfTar);
        Files.writeString(completeMarker, computeFingerprint(stagingDir));
    }

    private boolean isCachedTarValid(Path stagingDir) {
        if (!Files.exists(completeMarker) || !Files.isDirectory(localDmfTar)) {
            return false;
        }
        try {
            var expectedFingerprint = Files.readString(completeMarker).trim();
            if (expectedFingerprint.isEmpty()) {
                return false;
            }
            return expectedFingerprint.equals(computeFingerprint(stagingDir));
        }
        catch (IOException e) {
            log.warn("Could not verify cached dmftar for {}: {}", name, e.getMessage());
            return false;
        }
    }

    private String computeFingerprint(Path stagingDir) throws IOException {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            try (var stream = Files.walk(stagingDir)) {
                var sortedPaths = stream.sorted().toList();
                for (var path : sortedPaths) {
                    var relative = stagingDir.relativize(path).toString();
                    digest.update(relative.getBytes(StandardCharsets.UTF_8));
                    if (Files.isDirectory(path)) {
                        digest.update((byte) 1);
                    }
                    else {
                        digest.update((byte) 0);
                        var attrs = Files.readAttributes(path, BasicFileAttributes.class);
                        var buffer = ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES);
                        var mtime = attrs.lastModifiedTime().toInstant();
                        buffer.putLong(attrs.size());
                        buffer.putLong(mtime.getEpochSecond());
                        buffer.putInt(mtime.getNano());
                        digest.update(buffer.array());
                    }
                }
            }
            return Hex.encodeHexString(digest.digest());
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private void verifyRemoteCopy() throws IOException {
        var verify = config.getVerify();
        var result = commandRunner.runWithRetry(verify.getCommand(), placeholders, "Verifying " + name, verify.getInvalidOn());
        if (!result.succeeded() || (!verify.getInvalidOn().isEmpty() && result.output().contains(verify.getInvalidOn()))) {
            // Start from scratch on the next attempt
            try {
                deleteLocalDmfTar();
            }
            finally {
                runRemote("if [ -e %1$s ]; then chmod -R u+w %1$s 2>/dev/null || true; rm -rf %1$s; fi".formatted(quote(remoteTransferDir)),
                    "Removing invalid upload of " + name);
            }
            throw new IllegalStateException("Verification of uploaded " + name + " failed: " + result.output());
        }
    }

    /*
     * Can be repeated safely: if the verified copy has been moved already, it only removes the wrapper directory. An existing archive is moved aside while the new one is moved
     * into place, and only removed after that.
     */
    private void moveRemoteCopyIntoPlace() {
        var uploaded = quote(remoteTransferDir.resolve(name));
        var existing = quote(remoteDir.resolve(name));
        var backup = quote(remoteTransferDir.resolve(name + ".bak"));
        var transferDir = quote(remoteTransferDir);
        runRemote("if [ -e %1$s ]; then { if [ -e %2$s ]; then if [ -e %3$s ]; then chmod -R u+w %3$s 2>/dev/null || true; rm -rf %3$s; fi && mv %2$s %3$s; fi; } && mv %1$s %2$s; fi && if [ -e %4$s ]; then chmod -R u+w %4$s 2>/dev/null || true; rm -rf %4$s; fi"
            .formatted(uploaded, existing, backup, transferDir),
            "Moving " + name + " into place");
    }

    private void deleteLocalDmfTar() throws IOException {
        Files.deleteIfExists(completeMarker);
        if (Files.exists(localDmfTar)) {
            // dmftar makes everything readonly, even the directories, so we need to set write-permissions to be able to delete the files in them.
            try (var stream = Files.walk(localDmfTar)) {
                stream.filter(Files::isDirectory).forEach(path -> {
                    try {
                        Files.setPosixFilePermissions(path, DELETE_PERMISSIONS);
                    }
                    catch (IOException | UnsupportedOperationException e) {
                        log.warn("Failed to set delete permissions for {}: {}", path, e.getMessage());
                    }
                });
            }
        }
        FileUtils.deleteDirectory(localDmfTar.toFile());
    }

    private void runRemote(String script, String description) {
        commandRunner.runWithRetry(config.sshCommand(script), placeholders, description, null);
    }

    static String quote(Path path) {
        return "'" + path.toString().replace("'", "'\\''") + "'";
    }

    @Override
    public boolean fileExists(String filePath) {
        moratoriumManager.failIfUnderMoratorium();
        return remoteArchive.fileExists(filePath);
    }

    @Override
    public Iterator<Item> listAllItems() throws IOException {
        moratoriumManager.failIfUnderMoratorium();
        return remoteArchive.listAllItems();
    }
}
