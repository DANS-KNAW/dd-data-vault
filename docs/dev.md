Development
===========

Local testing
-------------
Local testing uses the same [set-up] as other DANS microservices.

[set-up]: https://dans-knaw.github.io/dans-module-archetype/common-practices/#debugging

Database for testing
--------------------
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

Testing with `dmftar`
---------------------

To test scenarios in which `dmftar` is used, install this command in a Python virtual environment:

1. `git submodule update` to get the correct version of the dmftar source code.
2. `python3 -m venv .venv` to create a virtual environment.
3. `source .venv/bin/activate` to activate the virtual environment.
4. `pushd modules/dmftar` to change directory to the dmftar source code.
5. `pip3 install -r requirements.txt` to install the dmftar dependencies.`
6. `flit install` to install the dmftar command.
7. `popd` to return to the root of the project.

(You could also use `pip3 install -e .` instead of `flit`, if you want to edit the source code, but that is not necessary).

Note that your public key must be added to the SURF Data Archive account used for testing.
