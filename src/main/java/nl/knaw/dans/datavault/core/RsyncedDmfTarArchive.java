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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.datavault.config.DmfTarArchiveProviderFactory;
import nl.knaw.dans.datavault.config.ExternalCommandConfig;
import nl.knaw.dans.layerstore.Archive;
import nl.knaw.dans.layerstore.Item;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An {@link Archive} that is a dmftar directory on a remote host, which is not written to the remote host directly by dmftar, but created locally first and then transferred with rsync,
 * so that interrupted connections can be resumed.
 * <p>
 * Archiving a layer takes these steps, each of which can be repeated after a failure or a restart of the service:
 * <ol>
 *     <li>create the dmftar directory in the local transfer root (a leftover complete one from an earlier attempt is reused);</li>
 *     <li>rsync it to the hidden directory {@value #REMOTE_TRANSFER_DIR_NAME} under the remote archive root, retrying until it is complete;</li>
 *     <li>run dmftar's verify on the remote copy;</li>
 *     <li>move the verified copy to its final place in the remote archive root, replacing an existing one;</li>
 *     <li>remove the local dmftar directory.</li>
 * </ol>
 * The archive only becomes visible in the remote archive root at step 4, so a partial transfer is never mistaken for an archive. The caller removes the staging directory after
 * {@link #archiveFrom(Path)} returns normally.
 * <p>
 * Reading from the archive is delegated to the given remote archive.
 */
@Slf4j
public class RsyncedDmfTarArchive implements Archive {
    public static final String REMOTE_TRANSFER_DIR_NAME = ".incoming";
    private static final String COMPLETE_MARKER_SUFFIX = ".complete";

    private final Archive remoteArchive;
    private final MoratoriumManager moratoriumManager;
    private final ExternalCommandRunner commandRunner;
    private final LocalDmfTarRunner localDmfTarRunner;
    private final DmfTarArchiveProviderFactory config;
    private final String name;
    private final Path remoteDir;
    private final Path remoteTransferDir;
    private final Path localDmfTar;
    private final Path completeMarker;
    private final Map<String, String> placeholders;

    @Getter
    private boolean archived;

    public RsyncedDmfTarArchive(long layerId, Archive remoteArchive, boolean archived, DmfTarArchiveProviderFactory config, ExternalCommandRunner commandRunner,
        LocalDmfTarRunner localDmfTarRunner, MoratoriumManager moratoriumManager) {
        this.remoteArchive = remoteArchive;
        this.archived = archived;
        this.config = config;
        this.commandRunner = commandRunner;
        this.localDmfTarRunner = localDmfTarRunner;
        this.moratoriumManager = moratoriumManager;
        this.name = layerId + ".dmftar";
        this.remoteDir = config.getArchiveRoot();
        this.remoteTransferDir = remoteDir.resolve(REMOTE_TRANSFER_DIR_NAME);
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

    @Override
    public void unarchiveTo(Path stagingDir) {
        moratoriumManager.failIfUnderMoratorium();
        remoteArchive.unarchiveTo(stagingDir);
    }

    @Override
    public void archiveFrom(Path stagingDir) {
        try {
            prepareLocalDmfTar(stagingDir);
            commandRunner.runWithRetry(config.getRsyncUpload(), placeholders, "Uploading " + name, null);
            verifyRemoteCopy();
            moveRemoteCopyIntoPlace();
            deleteLocalDmfTar();
            archived = true;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not archive " + stagingDir + " as " + name, e);
        }
    }

    private void prepareLocalDmfTar(Path stagingDir) throws IOException {
        if (Files.exists(completeMarker) && Files.isDirectory(localDmfTar)) {
            log.info("Reusing complete local dmftar {} from an earlier attempt", localDmfTar);
            return;
        }
        deleteLocalDmfTar();
        // A copy from an earlier attempt may be based on different content, which rsync does not clean up
        runRemote("rm -rf " + quote(remoteTransferDir.resolve(name)), "Removing stale upload of " + name);
        Files.createDirectories(localDmfTar.getParent());
        log.info("Creating local dmftar {}", localDmfTar);
        localDmfTarRunner.tarDirectory(stagingDir, localDmfTar);
        Files.createFile(completeMarker);
    }

    private void verifyRemoteCopy() throws IOException {
        var verify = config.getVerify();
        var result = commandRunner.runWithRetry(verify.getCommand(), placeholders, "Verifying " + name, verify.getInvalidOn());
        if (!result.succeeded()) {
            // Start from scratch on the next attempt
            runRemote("rm -rf " + quote(remoteTransferDir.resolve(name)), "Removing invalid upload of " + name);
            deleteLocalDmfTar();
            throw new IllegalStateException("Verification of uploaded " + name + " failed: " + result.output());
        }
    }

    /*
     * Can be repeated safely: if the verified copy has been moved already, it does nothing. An existing archive is moved aside while the new one is moved into place, and
     * only removed after that.
     */
    private void moveRemoteCopyIntoPlace() {
        var uploaded = quote(remoteTransferDir.resolve(name));
        var existing = quote(remoteDir.resolve(name));
        var backup = quote(remoteTransferDir.resolve(name + ".bak"));
        runRemote("if [ -e %1$s ]; then { if [ -e %2$s ]; then rm -rf %3$s && mv %2$s %3$s; fi; } && mv %1$s %2$s; fi && rm -rf %3$s".formatted(uploaded, existing, backup),
            "Moving " + name + " into place");
    }

    private void deleteLocalDmfTar() throws IOException {
        Files.deleteIfExists(completeMarker);
        FileUtils.deleteDirectory(localDmfTar.toFile());
    }

    private void runRemote(String script, String description) {
        var ssh = new ExternalCommandConfig(config.getSshExecutable().toString(), List.of("-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "${user}@${host}", script));
        commandRunner.runWithRetry(ssh, placeholders, description, null);
    }

    private static String quote(Path path) {
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
