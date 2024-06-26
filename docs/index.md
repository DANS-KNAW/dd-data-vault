dd-data-vault
=============

Manages a DANS Data Vault Storage Root.

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

Processing
-----------

