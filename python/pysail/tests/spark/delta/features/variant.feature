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
