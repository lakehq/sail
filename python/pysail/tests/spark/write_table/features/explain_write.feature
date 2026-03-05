Feature: EXPLAIN statement for write operations

  @sail-only
  Scenario: EXPLAIN CREATE TABLE AS SELECT shows physical plan with catalog command
    Given variable location for temporary directory explain_ctas
    When query template
      """
      EXPLAIN CREATE TABLE explain_ctas_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)
      """
    Then query plan matches snapshot
    Given final statement template
      """
      DROP TABLE IF EXISTS explain_ctas_table
      """

  @sail-only
  Scenario: EXPLAIN INSERT INTO shows physical plan with catalog command
    Given variable location for temporary directory explain_insert
    Given statement template
      """
      CREATE TABLE explain_insert_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1, 'Alice') AS t(id, name)
      """
    When query template
      """
      EXPLAIN INSERT INTO explain_insert_table
      SELECT * FROM VALUES (2, 'Bob') AS t(id, name)
      """
    Then query plan matches snapshot
    Given final statement
      """
      DROP TABLE IF EXISTS explain_insert_table
      """
