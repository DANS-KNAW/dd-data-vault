DANS BagPack Profile v0.1.0
===========================

Introduction
------------

### Version

* Document version: 0.1.0
* Publication date: N/A

### Status

The status of this document is DRAFT.

### Scope

This document specifies what constitutes a DANS BagPack in its most simple form. This version of the document is a preliminary release. The requirements in it
are non-normative and therefore formulated as "SHOULD"-rules. BagPacks that declare themselves as following the DANS BagPack Profile v0.1.0 are a best effort
and are not validated.

### Overview and Conventions

#### Keywords

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119]{:target=_blank}.

The key word "SHOULD" is also used to specify requirements that are impossible or impractical to check by the archival organization (i.e. DANS). The client
should do its best to meet these requirements, but not rely on their being validated by the archival organization.

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


Requirements
------------

### 1. RDA BagPack Related

The following items are required by the [RDA BagPack]{:target=_blank} specifications:

1. A DANS BagPack SHOULD be valid according to [BagIt v1.0]{:target=_blank}.
2. A DANS BagPack SHOULD contain a file `metadata/datacite.xml` (a) this file SHOULD be valid according to the
   [DataCite schema version 4.0 or later]{:target=_blank}, except for the requirement that there MUST be a DOI present: a DOI is not required for a DANS
   BagPack; (b) [DataCite's recommended properties]{:target=_blank} SHOULD be present.
3. Other files besides `datacite.xml` MAY be present in the `metadata` folder.
4. The files in the `metadata` folder SHOULD be mentioned in the `tag-manifest` (this is optional in BagIt, but required by RDA BagPack).
5. `BagIt-Profile-Identifier` SHOULD be provided.

### 2. Extra Requirements for DANS BagPack

The following items are required by the DANS BagPack Profile, in addition to the requirements of RDA BagPack:

1. `BagIt-Profile-Identifier` SHOULD contain `https://doi.org/10.17026/e948-0r32`.
2. There SHOULD be a file called `metadata/pid-mapping.txt`.
3. There SHOULD be a file called `metadata/oai-ore.jsonld`.

[RFC 2119]: {{ rfc_2119 }}
[BagIt v1.0]: {{ bagit }}
[RDA BagPack]: {{ rda_bagpack }}
[DataCite schema version 4.0 or later]: {{ datacite_4_0 }}
[DANS BagPack BagIt Profile]: {{ dans_bagpack_bagit_profile }}
[DataCite's recommended properties]: {{ levels_of_obligation }}