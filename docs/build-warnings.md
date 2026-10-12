Build warnings
==============

The following build warnings are not a problem and can be ignored. In general, we strive to have a warning-free build process. However, some warnings are
hard to suppress or resolve.

No application/json content media type found in response
========================================================

* **When**: `openapi-generator-maven-plugin:generate`
* **Full warning message**:

        [WARNING] No application/json content media type found in response. \
        Response examples can currently only be generated for application/json media type.

* **Explanation**: The response for the `/ocfl/objects/{id}/extension-file` is `application/octet-stream` and not `application/json`. The OpenAPI generator
  cannot generate response examples for this media type. There appears to be no way to explain to the plugin that this is OK.
