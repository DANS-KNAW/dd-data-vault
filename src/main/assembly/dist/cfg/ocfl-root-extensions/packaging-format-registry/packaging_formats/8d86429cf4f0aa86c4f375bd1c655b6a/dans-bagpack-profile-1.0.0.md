DANS BagPack Profile v1.0.0
===========================

Introduction
------------

### Version

* Document version: 1.0.0
* Publication date: N/A

### Status

The status of this document is DRAFT.

### Scope

This document specifies what constitutes an acceptable DANS BagPack. This includes all the requirements for a bag to be successfully processed by the DANS Data
Vault ingest workflow.

### Overview and Conventions

#### Keywords

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119]{:target=_blank}.

The key word "SHOULD" is also used to specify requirements that are impossible or impractical to check by the archival organization (i.e., DANS). The client
should do its best to meet these requirements but not rely on their being validated by the archival organization.

#### Subdivisions

The requirements are subdivided into the following sections:

* RDA BagPack Related - requirements that refer back to the [RDA BagPack]{:target=_blank} specifications. If a bag only needs to comply with the RDA BagPack
  specifications, then it should be sufficient to only check this section.
* Extra Requirements for DANS BagPack - requirements that are specific to the DANS BagPack Profile, and which are in addition to the RDA BagPack requirements.

The sections are numbered and may have numbered subsections. The requirements themselves are stated as numbered rules. Rules may have parts that are labeled
with letters: (a), (b), (c), etc. To uniquely identify a specific rule, use the notation

```
<section-nr>[.<subsection-nr>].<rule-nr> [(<letter>)]
```

Example: `2.3.4 (e)` means part **e** of the fourth rule in subsection 3 of section 2.

#### XML namespaces

When referring to XML element or attribute names or attribute values that have a prefix (such as `schema:name`) an element in a certain namespace is intended.
The table below lists the mapping from prefix to namespace. In the actual document, the namespace may be bound to a different prefix, or be the default
namespace.

| Prefix    | Namespace URI                                                       | Namespace documentation                          |
|-----------|---------------------------------------------------------------------|--------------------------------------------------|
| `schema`  | `http://schema.org/`                                                | [schema.org]{:target=_blank}                     |
| `dvcore`  | `https://dataverse.org/schema/core#`                                | Dataverse metadata elements                      |
| `vaultMd` | `https://schemas.dans.knaw.nl/metadatablock/dansDataVaultMetadata#` | [DANS Data Vault Metadata block]{:target=_blank} |

Requirements
------------

### 1. RDA BagPack Related

The following items are required by the [RDA BagPack]{:target=_blank} specifications:

1. A DANS BagPack MUST be valid according to [BagIt v1.0]{:target=_blank} or [BagIt v0.97]{:target=_blank}.
2. (a) A DANS BagPack MUST contain a file `metadata/datacite.xml` (b) this file MUST be valid according to the
   [DataCite schema version 4.0 or later]{:target=_blank}, except for the requirement that there MUST be a DOI present: a DOI is not required for a DANS
   BagPack; (c) [DataCite's recommended properties]{:target=_blank} SHOULD be present.
3. Other files besides `datacite.xml` MAY be present in the `metadata` folder.

### 2. Extra Requirements for DANS BagPack

The following items are required by the DANS BagPack Profile, in addition to the requirements of RDA BagPack:

1. The `bag-info.txt` file SHOULD contain an element `BagIt-Profile-Identifier` set to the identifier of the [DANS BagPack BagIt Profile]{:target=_blank}:
   `https://doi.org/10.17026/e948-0r32`.
2. (a) The bag MUST conform to the [DANS BagPack BagIt Profile]{:target=_blank} (even if the `BagIt-Profile-Identifier` element pointing to it is missing). (b)
   The bag SHOULD conform to any other BagIt profiles declared in the `BagIt-Profile-Identifier` element.
3. There MUST be a file called `metadata/pid-mapping.txt`: the structure of this file MUST be rows formatted as `<identifier>  <referenced object>`, where
   `<identifier>` is a unique URI and `<referenced object>` is the path to the file relative to the root of the bag, and both are separated by one or more
   spaces. One of the lines MAY be mapping from the dataset DOI to a folder directly under the `data` folder.
4. (a) There MUST a `metadata/oai-ore.json` file which MUST be a valid JSON-LD 1.0 or higher document; (b) The object described in the
   document MUST have the attribute `vaultMd:dansBagId` whose value is a URN:UUID. (c) The `ore:AggregatedResource`s of the `ore:Aggregation` MUST have the
   following attributes: (i) `@id` whose value is a URI; (ii) `schema:name`; (iii) `dvcore:restricted`, with value true or false.
5. There MUST be a one-to-one mapping between the files in the `data` folder and the files described in the Aggregation contained in  `oai-ore.jsonld` file:
   (a) all identifiers found in 2.4(c)(i) MUST be present in the left column of `pid-mapping.txt`; (b) the set of paths pointing to files found in the right
   column of `pid-mapping.txt` MUST be equal to the set of paths of files present in the `data` folder (relative to the bag root).

[RFC 2119]: https://www.ietf.org/rfc/rfc2119.txt
[BagIt v1.0]: https://www.rfc-editor.org/rfc/rfc8493
[BagIt v0.97]: https://tools.ietf.org/html/draft-kunze-bagit-14
[RDA BagPack]: http://doi.org/10.15497/RDA00025
[DataCite schema version 4.0 or later]: https://schema.datacite.org/meta/kernel-4.0/
[DANS BagPack BagIt Profile]: dans-bagpack-profile-1.0.0.json
[DataCite's recommended properties]: https://datacite-metadata-schema.readthedocs.io/en/latest/properties/overview/#levels-of-obligation
[schema.org]: https://schema.org/
[DANS Data Vault Metadata block]: https://docs.google.com/spreadsheets/d/1M0mefIC3VoXgQr-N_HlZlA6FAi_A_FAzbfCzngEEi3o/?gid=9637200#gid=9637200


