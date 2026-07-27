Feature: View types are expanded only at query output

  Background:
    Given variable location for temporary directory explain_parquet_view_types
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ location.sql }}
      USING parquet
      SELECT
        'top-level' AS label,
        named_struct(
          'name', 'nested',
          'aliases', array('left', 'right'),
          'payload', CAST('bytes' AS BINARY)
        ) AS details,
        map('first', 'value') AS tags
      """

  Scenario: EXPLAIN FORMATTED shows view types below the output boundary
    When query template
      """
      EXPLAIN FORMATTED
      SELECT label, details, tags
      FROM parquet.`{{ location.string }}`
      ORDER BY label
      """
    Then query plan matches snapshot
