Database for testing
====================

Although this project can use an HSQLDB database, this will only work for very trivial scenarios, due to limitations with respect to the BLOB type. It is
therefore recommended to use a PostgreSQL database, possibly on a vagrant VM.

Prepare the database for testing (on the vagrant VM):

```
sudo -u postgres psql
CREATE DATABASE dd_data_vault_local_test;
CREATE USER dd_data_vault_local_test WITH PASSWORD 'dd_data_vault_local_test';
GRANT ALL PRIVILEGES ON DATABASE dd_data_vault_local_test TO dd_data_vault_local_test;
\q
```

**UPDATE: for dev boxes created after 2025-10-31, this database is already present.**

Reset storage root
------------------

During testing, you will often want to reset the storage root to a clean state. For this you need to:

* Delete all the listing records from the database: 
  ```
  DELETE FROM listing_record;
  ```
* Remove the staging directory. Otherwise, the service will fail at startup because it cannot find the corresponding listing records:
  ```
  rm -rf data/vault/staging/*
  ```
* Restart the service.

