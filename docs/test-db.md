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
