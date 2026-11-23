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

import nl.knaw.dans.datavault.db.VaultObjectDao;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;
import nl.knaw.dans.layerstore.LayeredItemStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObjectBlockingTest {
    private final VaultObjectDao dao = mock(VaultObjectDao.class);
    private final OcflRepositoryProvider provider = OcflRepositoryProvider.builder()
        .itemStore(mock(LayeredItemStore.class))
        .layerConsistencyChecker(mock(LayerConsistencyChecker.class))
        .workDir(Path.of("work"))
        .vaultObjectDao(dao)
        .build();

    @Test
    void failed_first_import_creates_blocked_uningested_record() {
        var result = provider.importObject("object", p -> { throw new IllegalStateException("failed"); });
        assertThat(result).isPresent();
        var object = ArgumentCaptor.forClass(VaultObject.class);
        verify(dao).lockObject("object");
        verify(dao).save(object.capture());
        assertThat(object.getValue().isBlocked()).isTrue();
        assertThat(object.getValue().isIngested()).isFalse();
    }

    @Test
    void blocked_object_is_not_imported() {
        when(dao.findByObjectId("object")).thenReturn(Optional.of(VaultObject.builder().blocked(true).build()));
        var result = provider.importObject("object", p -> { throw new AssertionError("must not run"); });
        assertThat(result).hasValueSatisfying(e -> assertThat(e).hasMessageContaining("blocked"));
        verify(dao, never()).save(any());
    }

    @Test
    void clearing_block_keeps_ingestion_state() {
        var object = VaultObject.builder().blocked(true).ingested(false).build();
        when(dao.findByObjectId("object")).thenReturn(Optional.of(object));
        assertThat(provider.clearObjectBlock("object")).contains(false);
        assertThat(object.isBlocked()).isFalse();
        assertThat(object.isIngested()).isFalse();
        verify(dao).lockObject("object");
    }
}