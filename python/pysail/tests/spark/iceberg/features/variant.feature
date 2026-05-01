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
    Then iceberg metadata contains
      | path                      | value     |
      | format-version            | 3         |
      | schemas[0].fields[1].type | "variant" |
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
