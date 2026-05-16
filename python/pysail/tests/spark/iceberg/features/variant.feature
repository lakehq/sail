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
    Then iceberg latest metadata file is v1.metadata.json
    Then iceberg version hint is 1
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
    Then iceberg latest metadata file is v2.metadata.json
    Then iceberg version hint is 2
    Then iceberg current manifest list matches snapshot
