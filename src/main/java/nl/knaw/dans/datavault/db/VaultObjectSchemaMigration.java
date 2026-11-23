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

import org.hibernate.SessionFactory;

public class VaultObjectSchemaMigration {
    public static void migrate(SessionFactory sessionFactory) {
        try (var session = sessionFactory.openSession()) {
            var transaction = session.beginTransaction();
            try {
                session.doWork(connection -> {
                    if (!"PostgreSQL".equals(connection.getMetaData().getDatabaseProductName())) {
                        return;
                    }
                    try (var statement = connection.createStatement()) {
                        statement.execute("LOCK TABLE vault_object IN ACCESS EXCLUSIVE MODE");
                        statement.execute("ALTER TABLE vault_object ADD COLUMN IF NOT EXISTS blocked boolean");
                        statement.execute("ALTER TABLE vault_object ADD COLUMN IF NOT EXISTS ingested boolean");
                        statement.executeUpdate("UPDATE vault_object SET blocked = false WHERE blocked IS NULL");
                        statement.executeUpdate("UPDATE vault_object SET ingested = true WHERE ingested IS NULL");
                        statement.execute("ALTER TABLE vault_object ALTER COLUMN blocked SET DEFAULT false, ALTER COLUMN blocked SET NOT NULL");
                        statement.execute("ALTER TABLE vault_object ALTER COLUMN ingested SET DEFAULT false, ALTER COLUMN ingested SET NOT NULL");
                    }
                });
                transaction.commit();
            }
            catch (Exception e) {
                transaction.rollback();
                throw e;
            }
        }
    }
}