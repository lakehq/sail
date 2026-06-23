@spark-4
Feature: Delta Lake Variant support

  Scenario: Write and read a Variant column
    Given variable location for temporary directory delta_variant
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_table
      """
    Given statement template
      """
      CREATE TABLE delta_variant_table (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_variant_table
      SELECT 1, parse_json('{"a":1,"b":"delta"}')
      """
    Then delta log first commit protocol and metadata contains
      | path                                 | value           |
      | protocol.minReaderVersion            | 3               |
      | protocol.minWriterVersion            | 7               |
      | protocol.readerFeatures              | ["variantType"] |
      | protocol.writerFeatures              | ["variantType", "appendOnly", "invariants"] |
      | metaData.schemaString.fields[1].type | "variant"       |
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b,
        to_json(payload) AS payload_json
      FROM delta_variant_table
      ORDER BY id
      """
    Then query result ordered
      | id | a | b     | payload_json        |
      | 1  | 1 | delta | {"a":1,"b":"delta"} |

  Scenario: Write and read a Variant column with preview VariantType feature
    Given variable location for temporary directory delta_variant_preview
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_preview_table
      """
    Given statement template
      """
      CREATE TABLE delta_variant_preview_table (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported')
      """
    Given statement
      """
      INSERT INTO delta_variant_preview_table
      SELECT 1, parse_json('{"a":1,"b":"preview"}')
      """
    Then delta log first commit protocol and metadata contains
      | path                                 | value           |
      | protocol.minReaderVersion            | 3               |
      | protocol.minWriterVersion            | 7               |
      | protocol.readerFeatures              | ["variantType-preview"] |
      | protocol.writerFeatures              | ["variantType-preview", "appendOnly", "invariants"] |
      | metaData.schemaString.fields[1].type | "variant"       |
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b
      FROM delta_variant_preview_table
      ORDER BY id
      """
    Then query result ordered
      | id | a | b       |
      | 1  | 1 | preview |

  Scenario: Create table with VariantShredding table property
    Given variable location for temporary directory delta_variant_shredding_protocol
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_protocol
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_protocol (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_protocol
      SELECT 1, parse_json('{"a":1}')
      """
    Then delta log first commit protocol and metadata contains
      | path                                  | value           |
      | protocol.minReaderVersion             | 3               |
      | protocol.minWriterVersion             | 7               |
      | protocol.readerFeatures               | ["variantType", "variantShredding-preview"] |
      | protocol.writerFeatures               | ["variantType", "appendOnly", "invariants", "variantShredding-preview"] |
      | metaData.configuration['delta.enableVariantShredding'] | "true" |
      | metaData.schemaString.fields[1].type  | "variant"       |
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        to_json(payload) AS payload_json
      FROM delta_variant_shredding_protocol
      ORDER BY id
      """
    Then query result ordered
      | id | a | payload_json |
      | 1  | 1 | {"a":1}      |

  Scenario: VariantShredding write round trips
    Given variable location for temporary directory delta_variant_shredding_roundtrip
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_roundtrip
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_roundtrip (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_roundtrip
      SELECT * FROM VALUES
        (1, parse_json('{"a":1,"b":"delta"}')),
        (2, parse_json('{"a":2,"b":"lake"}'))
      """
    Then delta log first commit protocol and metadata matches snapshot
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b
      FROM delta_variant_shredding_roundtrip
      ORDER BY id
      """
    Then query result ordered
      | id | a | b     |
      | 1  | 1 | delta |
      | 2  | 2 | lake  |

  Scenario: VariantShredding write round trips on metadata-as-data path
    Given variable location for temporary directory delta_variant_shredding_metadata_read
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_metadata_read
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_metadata_read (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      OPTIONS (metadataAsDataRead 'true')
      TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_metadata_read
      SELECT * FROM VALUES
        (1, parse_json('{"a":1,"b":"metadata"}')),
        (2, parse_json('{"a":2,"b":"path"}'))
      """
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b
      FROM delta_variant_shredding_metadata_read
      ORDER BY id
      """
    Then query result ordered
      | id | a | b        |
      | 1  | 1 | metadata |
      | 2  | 2 | path     |

  Scenario: Create non-Variant table with VariantShredding table property
    Given variable location for temporary directory delta_variant_shredding_no_variant
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_no_variant
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_no_variant (
        id INT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_no_variant
      SELECT 1
      """
    Then delta log first commit protocol and metadata contains
      | path                                  | value           |
      | protocol.minReaderVersion             | 3               |
      | protocol.minWriterVersion             | 7               |
      | protocol.readerFeatures               | ["variantShredding-preview"] |
      | protocol.writerFeatures               | ["variantShredding-preview", "appendOnly", "invariants"] |
      | metaData.configuration['delta.enableVariantShredding'] | "true" |
      | metaData.schemaString.fields[0].type  | "integer"       |
    When query
      """
      SELECT id
      FROM delta_variant_shredding_no_variant
      ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |

  Scenario: Write and read a table with stable VariantShredding feature
    Given variable location for temporary directory delta_variant_shredding_stable
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_stable
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_stable (
        id INT,
        payload VARIANT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.feature.variantShredding' = 'supported')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_stable
      SELECT * FROM VALUES
        (1, parse_json('{"a":1,"b":"stable"}')),
        (2, parse_json('{"a":2,"b":"feature"}'))
      """
    Then delta log first commit protocol and metadata contains
      | path                                 | value           |
      | protocol.minReaderVersion            | 3               |
      | protocol.minWriterVersion            | 7               |
      | protocol.readerFeatures              | ["variantType", "variantShredding"] |
      | protocol.writerFeatures              | ["variantType", "appendOnly", "invariants", "variantShredding"] |
      | metaData.schemaString.fields[1].type | "variant"       |
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b
      FROM delta_variant_shredding_stable
      ORDER BY id
      """
    Then query result ordered
      | id | a | b       |
      | 1  | 1 | stable  |
      | 2  | 2 | feature |

  Scenario: Create non-Variant table with stable VariantShredding feature
    Given variable location for temporary directory delta_variant_shredding_stable_no_variant
    Given final statement
      """
      DROP TABLE IF EXISTS delta_variant_shredding_stable_no_variant
      """
    Given statement template
      """
      CREATE TABLE delta_variant_shredding_stable_no_variant (
        id INT
      )
      USING DELTA
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('delta.feature.variantShredding' = 'supported')
      """
    Given statement
      """
      INSERT INTO delta_variant_shredding_stable_no_variant
      SELECT 1
      """
    Then delta log first commit protocol and metadata contains
      | path                                 | value           |
      | protocol.minReaderVersion            | 3               |
      | protocol.minWriterVersion            | 7               |
      | protocol.readerFeatures              | ["variantShredding"] |
      | protocol.writerFeatures              | ["variantShredding", "appendOnly", "invariants"] |
      | metaData.schemaString.fields[0].type | "integer"       |
    When query
      """
      SELECT id
      FROM delta_variant_shredding_stable_no_variant
      ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
