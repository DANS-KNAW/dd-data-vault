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

import nl.knaw.dans.datavault.config.DmfTarArchiveProviderFactory;
import nl.knaw.dans.layerstore.Archive;
import nl.knaw.dans.layerstore.ArchiveProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides {@link RsyncedDmfTarArchive}s. Operations on the remote archive root are delegated to the given provider, which also creates the archives that are read from.
 */
public class RsyncedDmfTarArchiveProvider implements ArchiveProvider {
    // ls -F appends a / to directories
    private static final Pattern VALID_ENTRY = Pattern.compile("^\\d{13,}\\.dmftar(\\" + RsyncedDmfTarArchive.PARTIAL_SUFFIX + ")?/$");
    private static final Pattern LAYER_ARCHIVE_ENTRY = Pattern.compile("^(\\d+)\\.dmftar$");

    private final ArchiveProvider remoteProvider;
    private final DmfTarArchiveProviderFactory config;
    private final Semaphore unarchiveSlots;
    private final ExternalCommandRunner commandRunner;
    private final LocalDmfTarRunner localDmfTarRunner;
    private final MoratoriumManager moratoriumManager;

    public RsyncedDmfTarArchiveProvider(ArchiveProvider remoteProvider, DmfTarArchiveProviderFactory config, Semaphore unarchiveSlots, ExternalCommandRunner commandRunner,
        LocalDmfTarRunner localDmfTarRunner,
        MoratoriumManager moratoriumManager) {
        this.remoteProvider = remoteProvider;
        this.config = config;
        this.unarchiveSlots = unarchiveSlots;
        this.commandRunner = commandRunner;
        this.localDmfTarRunner = localDmfTarRunner;
        this.moratoriumManager = moratoriumManager;
    }

    public boolean isUnderMoratorium() {
        return moratoriumManager.isUnderMoratorium();
    }

    @Override
    public Archive createArchive(long layerId) {
        return new RsyncedDmfTarArchive(layerId, remoteProvider.createArchive(layerId), config, commandRunner, localDmfTarRunner, moratoriumManager, unarchiveSlots);
    }

    @Override
    public boolean exists(long layerId) {
        var remotePath = config.getArchiveRoot().resolve(layerId + ".dmftar");
        var result = commandRunner.run(config.sshCommand("test -e " + RsyncedDmfTarArchive.quote(remotePath)),
            Map.of("user", config.getUser(), "host", config.getHost()));
        moratoriumManager.failIfUnderMoratorium();
        if (result.exitCode() == 0) {
            return true;
        }
        if (result.exitCode() == 1) {
            return false;
        }
        throw new CommandFailedException("Checking existence of " + layerId + ".dmftar", result.exitCode(), result.output());
    }

    @Override
    public List<Long> listLayerIds() throws IOException {
        var remoteDir = config.getArchiveRoot();
        var result = commandRunner.runWithRetry(config.sshCommand("ls -1 " + RsyncedDmfTarArchive.quote(remoteDir)),
            Map.of("user", config.getUser(), "host", config.getHost()), "Listing " + remoteDir, null);
        return result.output().lines()
            .map(String::strip)
            .map(LAYER_ARCHIVE_ENTRY::matcher)
            .filter(Matcher::matches)
            .map(m -> Long.valueOf(m.group(1)))
            .toList();
    }

    /**
     * Checks that the remote archive root contains only archives and the <code>.partial</code> directories of archives that are being transferred.
     */
    @Override
    public void validateRoot() {
        var remoteDir = config.getArchiveRoot();
        var result = commandRunner.runWithRetry(config.sshCommand("ls -1 -F " + RsyncedDmfTarArchive.quote(remoteDir)),
            Map.of("user", config.getUser(), "host", config.getHost()), "Listing " + remoteDir, null);
        var illegalFiles = result.output().lines()
            .filter(line -> !line.isBlank() && !VALID_ENTRY.matcher(line).matches())
            .toList();
        if (!illegalFiles.isEmpty()) {
            throw new IllegalStateException(String.format("Archive root '%s' on '%s' contains illegal files: %s", remoteDir, config.getHost(), String.join(", ", illegalFiles)));
        }
    }
}
