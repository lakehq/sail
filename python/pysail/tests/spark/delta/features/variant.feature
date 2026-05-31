@sail-only @variant @spark-4
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
