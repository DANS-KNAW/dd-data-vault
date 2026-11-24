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
package nl.knaw.dans.datavault.db;

import io.dropwizard.testing.junit5.DAOTestExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import nl.knaw.dans.datavault.core.VaultObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
public class VaultObjectDaoTest {

    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder()
        .addEntityClass(VaultObject.class)
        .build();

    private VaultObjectDao dao;

    @BeforeEach
    public void setUp() {
        dao = new VaultObjectDao(daoTestRule.getSessionFactory());
    }

    @Test
    public void save_and_findByObjectId_should_persist_and_retrieve_object() {
        var now = Instant.now();
        var obj = VaultObject.builder()
            .objectId("urn:uuid:test-1")
            .createdAt(now)
            .updatedAt(now)
            .build();

        daoTestRule.inTransaction(() -> dao.save(obj));

        var retrieved = daoTestRule.inTransaction(() -> dao.findByObjectId("urn:uuid:test-1"));
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().getObjectId()).isEqualTo("urn:uuid:test-1");
        assertThat(retrieved.get().getId()).isNotNull();

        var nonExistent = daoTestRule.inTransaction(() -> dao.findByObjectId("urn:uuid:non-existent"));
        assertThat(nonExistent).isEmpty();
    }

    @Test
    public void existsByObjectId_should_return_correct_boolean() {
        var now = Instant.now();
        daoTestRule.inTransaction(() -> dao.save(VaultObject.builder()
            .objectId("urn:uuid:obj-a")
            .createdAt(now)
            .updatedAt(now)
            .build()));

        var exists = daoTestRule.inTransaction(() -> dao.existsByObjectId("urn:uuid:obj-a"));
        var notExists = daoTestRule.inTransaction(() -> dao.existsByObjectId("urn:uuid:obj-b"));

        assertThat(exists).isTrue();
        assertThat(notExists).isFalse();
    }

    @Test
    public void findObjectIds_should_return_ids_in_alphabetical_order_with_pagination() {
        var now = Instant.now();
        var objects = List.of(
            VaultObject.builder().objectId("urn:uuid:c").createdAt(now).updatedAt(now).ingested(true).build(),
            VaultObject.builder().objectId("urn:uuid:a").createdAt(now).updatedAt(now).ingested(true).build(),
            VaultObject.builder().objectId("urn:uuid:e").createdAt(now).updatedAt(now).ingested(true).build(),
            VaultObject.builder().objectId("urn:uuid:b").createdAt(now).updatedAt(now).ingested(true).build(),
            VaultObject.builder().objectId("urn:uuid:d").createdAt(now).updatedAt(now).ingested(true).build()
        );

        daoTestRule.inTransaction(() -> dao.saveAll(objects));

        var count = daoTestRule.inTransaction(() -> dao.count());
        assertThat(count).isEqualTo(5);

        // All objects ordered
        var all = daoTestRule.inTransaction(() -> dao.findObjectIds(null, null));
        assertThat(all).containsExactly("urn:uuid:a", "urn:uuid:b", "urn:uuid:c", "urn:uuid:d", "urn:uuid:e");

        // First page
        var page1 = daoTestRule.inTransaction(() -> dao.findObjectIds(2, 0));
        assertThat(page1).containsExactly("urn:uuid:a", "urn:uuid:b");

        // Second page
        var page2 = daoTestRule.inTransaction(() -> dao.findObjectIds(2, 2));
        assertThat(page2).containsExactly("urn:uuid:c", "urn:uuid:d");

        // Partial final page
        var page3 = daoTestRule.inTransaction(() -> dao.findObjectIds(2, 4));
        assertThat(page3).containsExactly("urn:uuid:e");

        // Offset beyond count
        var beyond = daoTestRule.inTransaction(() -> dao.findObjectIds(10, 100));
        assertThat(beyond).isEmpty();

        // Limit zero
        var limitZero = daoTestRule.inTransaction(() -> dao.findObjectIds(0, 0));
        assertThat(limitZero).isEmpty();
    }

    @Test
    public void listing_filters_ingested_and_blocked_records_before_pagination() {
        var now = Instant.now();
        daoTestRule.inTransaction(() -> dao.saveAll(List.of(
            VaultObject.builder().objectId("a").createdAt(now).updatedAt(now).ingested(true).build(),
            VaultObject.builder().objectId("b").createdAt(now).updatedAt(now).ingested(true).blocked(true).build(),
            VaultObject.builder().objectId("c").createdAt(now).updatedAt(now).blocked(true).build(),
            VaultObject.builder().objectId("d").createdAt(now).updatedAt(now).build()
        )));
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null))).containsExactly("a", "b", "c", "d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, null, null))).containsExactly("a", "b", "c", "d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, null, true))).containsExactly("a", "b");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, null, false))).containsExactly("c", "d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, true, null))).containsExactly("b", "c");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, false, null))).containsExactly("a", "d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, true, true))).containsExactly("b");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, true, false))).containsExactly("c");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, false, true))).containsExactly("a");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(null, null, false, false))).containsExactly("d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(1, 1, null, false))).containsExactly("d");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(1, 1, true, null))).containsExactly("c");
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(0, 0, false, false))).isEmpty();
        assertThat(daoTestRule.inTransaction(() -> dao.findObjectIds(1, 1, false, false))).isEmpty();
    }
}
