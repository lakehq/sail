@sail-only @variant @spark-4
Feature: Iceberg Variant support

  Scenario: Write and read a Variant column
    Given variable location for temporary directory iceberg_variant
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_variant_table
      """
    Given statement template
      """
      CREATE TABLE iceberg_variant_table (
        id INT,
        payload VARIANT
      )
      USING iceberg
      LOCATION {{ location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_variant_table
      SELECT 1, parse_json('{"a":2,"b":"iceberg"}')
      """
    Then iceberg metadata matches snapshot
    Then iceberg latest metadata file is v2.metadata.json
    Then iceberg version hint is 2
    Then iceberg current manifest list matches snapshot
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b,
        to_json(payload) AS payload_json
      FROM iceberg_variant_table
      ORDER BY id
      """
    Then query result ordered
      | id | a | b       | payload_json          |
      | 1  | 2 | iceberg | {"a":2,"b":"iceberg"} |

  Scenario: Write and read a Variant column with Parquet variant shredding
    Given variable location for temporary directory iceberg_variant_shredding_roundtrip
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_variant_shredding_roundtrip_table
      """
    Given statement template
      """
      CREATE TABLE iceberg_variant_shredding_roundtrip_table (
        id INT,
        payload VARIANT
      )
      USING iceberg
      LOCATION {{ location.uri }}
      TBLPROPERTIES (
        'format-version' = '3',
        'write.parquet.shred-variants' = 'true',
        'write.parquet.variant-inference-buffer-size' = '2'
      )
      """
    Given statement
      """
      INSERT INTO iceberg_variant_shredding_roundtrip_table
      SELECT 1, parse_json('{"a":2,"b":"iceberg","nested":{"c":7}}')
      UNION ALL
      SELECT 2, parse_json('{"a":5,"b":"sail","nested":{"c":9}}')
      """
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.b', 'string') AS b,
        variant_get(payload, '$.nested.c', 'int') AS c,
        to_json(payload) AS payload_json
      FROM iceberg_variant_shredding_roundtrip_table
      ORDER BY id
      """
    Then query result ordered
      | id | a | b       | c | payload_json                                |
      | 1  | 2 | iceberg | 7 | {"a":2,"b":"iceberg","nested":{"c":7}} |
      | 2  | 5 | sail    | 9 | {"a":5,"b":"sail","nested":{"c":9}}    |

  Scenario: Read a shredded Variant column through metadata-as-data
    Given variable location for temporary directory iceberg_variant_shredding_metadata_read
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_variant_shredding_metadata_read_table
      """
    Given statement template
      """
      CREATE TABLE iceberg_variant_shredding_metadata_read_table (
        id INT,
        payload VARIANT
      )
      USING iceberg
      LOCATION {{ location.uri }}
      OPTIONS (metadataAsDataRead 'true')
      TBLPROPERTIES (
        'format-version' = '3',
        'write.parquet.shred-variants' = 'true',
        'write.parquet.variant-inference-buffer-size' = '2'
      )
      """
    Given statement
      """
      INSERT INTO iceberg_variant_shredding_metadata_read_table
      SELECT 1, parse_json('{"a":2,"b":"iceberg","nested":{"c":7}}')
      UNION ALL
      SELECT 2, parse_json('{"a":5,"b":"sail","nested":{"c":9}}')
      """
    When query
      """
      SELECT
        id,
        variant_get(payload, '$.a', 'int') AS a,
        variant_get(payload, '$.nested.c', 'int') AS c,
        to_json(payload) AS payload_json
      FROM iceberg_variant_shredding_metadata_read_table
      ORDER BY id
      """
    Then query result ordered
      | id | a | c | payload_json                                |
      | 1  | 2 | 7 | {"a":2,"b":"iceberg","nested":{"c":7}} |
      | 2  | 5 | 9 | {"a":5,"b":"sail","nested":{"c":9}}    |

  Scenario: Append a Variant column with mergeSchema upgrades the table format version
    Given variable location for temporary directory iceberg_variant_schema_evolution
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_variant_schema_evolution_table
      """
    Given statement template
      """
      CREATE TABLE iceberg_variant_schema_evolution_table (
        id INT
      )
      USING iceberg
      LOCATION {{ location.uri }}
      """
    Given statement
      """
      INSERT INTO iceberg_variant_schema_evolution_table
      SELECT CAST(1 AS INT)
      """
    Then iceberg metadata matches snapshot
    Given append query to iceberg table in location with mergeSchema
      """
      SELECT CAST(2 AS INT) AS id, parse_json('{"a":3,"b":"merge"}') AS payload
      """
    Then iceberg metadata matches snapshot
    Then iceberg latest metadata file is v3.metadata.json
    Then iceberg version hint is 3
    Then iceberg current manifest list matches snapshot
