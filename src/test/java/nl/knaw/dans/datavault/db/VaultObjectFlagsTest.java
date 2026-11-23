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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(DropwizardExtensionsSupport.class)
class VaultObjectFlagsTest {
    public final DAOTestExtension daoTestRule = DAOTestExtension.newBuilder().addEntityClass(VaultObject.class).build();

    @Test
    void flags_default_to_false_and_survive_updates() {
        var dao = new VaultObjectDao(daoTestRule.getSessionFactory());
        var now = Instant.now();
        daoTestRule.inTransaction(() -> dao.save(VaultObject.builder().objectId("flags").createdAt(now).updatedAt(now).build()));
        daoTestRule.inTransaction(() -> {
            var object = dao.findByObjectId("flags").orElseThrow();
            assertThat(object.isBlocked()).isFalse();
            assertThat(object.isIngested()).isFalse();
            object.setBlocked(true);
            object.setIngested(true);
        });
        daoTestRule.inTransaction(() -> {
            var object = dao.findByObjectId("flags").orElseThrow();
            assertThat(object.isBlocked()).isTrue();
            assertThat(object.isIngested()).isTrue();
        });
    }
}