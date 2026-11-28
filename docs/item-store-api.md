About using the Item Store API
===============================

In normal usage the object import interface, described on [the main page](./index.md), is pretty much everything you need. However, there are times when it
may be necessary to edit the layer store on a lower level. As described in the documentation of the
underlying [dans-layer-store-lib]{:target=_blank}, the layer store is conceptually just a file/folder
hierarchy, or **item store**. The service contains API endpoints that let you perform editing operations directly on the item store.

!!! warning "Corruption of the OCFL repository is possible"

    Be aware that, when modifying the item store this way, it is possible to corrupt the OCFL repository. That is why these end-points must be enabled explicitly
    to become available. You can do so by editing the `config.yml` file and restarting the service. It is recommended to reset all the item store endpoints
    to "disabled" after you have finished using them to prevent accidental use. Also, make sure that no imports are running concurrently when doing these 
    low-level edits. You should shut down `dd-transfer-to-vault` before starting your edits and check that there are no pending imports in the queue.

### The exchange directory

The endpoints that copy files between the local file system and the item store (`copy-directory-into`, `copy-file-into` and `copy-file-out-of`) only
accept local paths inside the **exchange directory**, configured as `dataVault.itemstore.exchangeDir` (by default `/data/vault/exchange`; the Ansible
role creates `exchange` in the instance's data directory). Put files that are to be copied into the item store in this directory, and retrieve files
that have been copied out of it from there. Paths outside this directory, including paths that lead outside it via `..` or symbolic links, are refused.
A directory to be copied into the item store must not contain symbolic links, and `copy-file-out-of` does not overwrite existing files.

!!! tip "Changing object version properties"

    Do not use the item store API to replace `object_version_properties.json`. Use the `/object-version-properties` endpoints of the [API](./to-api.md)
    or the [data-vault object-property]{:target=_blank} command instead. They validate the change against the property registry and the schema, and
    update the checksum sidecar file.

[data-vault object-property]: {{ data_vault_cli_object_properties_url }}
[dans-layer-store-lib]: https://dans-knaw.github.io/dans-layer-store-lib/#the-itemstore-interface