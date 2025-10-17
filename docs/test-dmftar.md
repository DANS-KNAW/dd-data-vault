Testing with `dmftar`
=====================

To test scenarios in which `dmftar` is used, install this command in a Python virtual environment:

1. `git submodule update` to get the correct version of the dmftar source code.
2. `python3 -m venv .venv` to create a virtual environment.
3. `source .venv/bin/activate` to activate the virtual environment.
4. `pushd modules/dmftar` to change directory to the dmftar source code.
5. `pip3 install -r requirements.txt` to install the dmftar dependencies.
6. `flit install` to install the dmftar command.
7. `popd` to return to the root of the project.

(You could also use `pip3 install -e .` instead of `flit`, if you want to edit the source code, but that is not necessary).

Note that your public key must be added to the SURF Data Archive account used for testing.
