Feature: simple sort test

  @sail-only
  Scenario: simple presorted
    Given variable location for temporary directory simple_presorted
    Given final statement
      """
      DROP TABLE IF EXISTS simple_presorted_t
      """
    Given statement template
      """
      CREATE TABLE simple_presorted_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (3), (1), (2) AS t(col) ORDER BY col
      """
    When query
      """
      EXPLAIN SELECT col FROM simple_presorted_t ORDER BY col
      """
    Then query plan matches snapshot
