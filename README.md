dd-data-vault
===========
![Build Status](https://github.com/DANS-KNAW/dd-data-vault/actions/workflows/build.yml/badge.svg)
[![codecov](https://codecov.io/gh/DANS-KNAW/dd-data-vault/branch/master/graph/badge.svg)](https://codecov.io/gh/DANS-KNAW/dd-data-vault)
![Site Status](https://github.com/DANS-KNAW/dd-data-vault/actions/workflows/docs.yml/badge.svg)

For documentation see: https://dans-knaw.github.io/dd-data-vault

### Import recovery and upgrade

On startup, PostgreSQL installations are upgraded with object block and ingestion state.
Stop old service instances before upgrading so that they cannot write during the migration.
Failed object imports are blocked until an operator clears the block. Before clearing one,
inspect the failed outbox and reconcile the OCFL object's actual versions with the intended
retry; clearing a block does not repair or remove versions.

OCFL filesystem changes and database transactions are not atomic. In particular, if the
database transaction fails or the service crashes after an OCFL write, the block itself may
not be recorded. Stop imports and reconcile affected objects before resuming after such errors.
