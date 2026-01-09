dd-data-vault
=============

Manages a DANS Data Vault Storage Root

Purpose
-------
A DANS Data Vault Storage Root is an OCFL storage root that is used to store a collection of long term preservation objects.

Interfaces
----------

### Batches and Object Import Directories

Objects versions to be stored must be placed under the inbox in a batch directory. The layout of the batch directory is as follows:

```plaintext
batch-dir
 ├── urn:nbn:nl:ui:13-26febff0-4fd4-4ee7-8a96-b0703b96f812
 │   ├── v1
 │   │   └── <content files>
 │   ├── v2
 │   │   └── <content files>
 │   ├── v2.properties
 │   └── v3
 │       └── <content files>
 ├── urn:nbn:nl:ui:13-2ced2354-3a9d-44b1-a594-107b3af99789
 │   └── v3
 │       └── <content files>
 └── urn:nbn:nl:ui:13-b7c0742f-a9b2-4c11-bffe-615dbe24c8a0
      └── v1
          └── <content files> 
```

* `batch-dir` - The batch directory is the directory where the batch of objects to be imported is placed.
* `urn:nbn:nl:ui:13-26febff0-4fd4-4ee7-8a96-b0703b96f812` - The directory name is the identifier of the object. The pattern that an
  identifier must match can be configured in the configuration file.
* `v1`, `v2`, `v3` - The version directories contain the content of the object version. The version directories must be named `v1`, `v2`, `v3`, etc.
  When updating an existing object, the first version directory must be named after the next version to be created in the OCFL object.
* The service can also be configured to accept timestamps as version directories. In that case, the version directories are expected to be numbers,
  representing the timestamp of the version in milliseconds since the epoch. This timestamp is only used for ordering the versions in the OCFL object, so
  any number can be used as long as it is unique for the object. This option is mainly used for testing purposes.
* A version directory can optionally be accompanied by a Java properties file named `vN.properties`, where `N` is the version number, (e.g. `v2.properties` for
  version 2).
  This properties-file - if present - must have the following properties:
    * `user.name` - the name of the user that created this version
    * `user.email` - the email of the user that created this version
    * `message` - the commit message for this version
      If no properties file is present, the service will use default values for these properties. These default values can be configured in the configuration
      file, under `dataVault.defaultVersionInfo`. If no default values are configured, an error will be raised.
* `vN.properties` can optionally have custom properties. These are properties prefixed with the string `custom.`. The service will add these as object version
  properties using the mechanism defined by the [Object Version Properties]{:target=_blank} extension. 

[Object Version Properties]: {{ object_version_properties_ext }}

* Processing
----------

### Order of batches

To ensure that updates for one object are processed in the correct order, the service will process all batches in the inbox in the order they were received.
Otherwise, it would be possible that a later batch would overtake an earlier batch. If these two batches contain updates for the same object, this would lead to
an error because the version directory would not coincide with the next expected version in the OCFL object.

### Parallelization of object import directory processing

Per batch the object import directory processing can be parallelized because there can be only one object import directory per object in a batch. The task that
processes the object import directory ensures that the version directories are processed in the correct order.

### Automatic layer creation

After processing a batch, the service will check if the maximum size of the layer has been reached. If this is the case, the service will create a new layer
and start the archiving process of the old layer. Since object import directories are processed in parallel, it is not possible to do a more fine-grained
check for the maximum size of the layer.


