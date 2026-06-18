Feature: EXPLAIN statement for write operations

  @sail-only
  Scenario: EXPLAIN CREATE TABLE AS SELECT shows physical plan with catalog command
    Given variable location for temporary directory explain_ctas
    Given final statement
      """
      DROP TABLE IF EXISTS explain_ctas_table
      """
    When query template
      """
      EXPLAIN CREATE TABLE explain_ctas_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)
      """
    Then query plan matches snapshot

  @sail-only
  Scenario: EXPLAIN INSERT INTO shows physical plan with catalog command
    Given variable location for temporary directory explain_insert
    Given final statement
      """
      DROP TABLE IF EXISTS explain_insert_table
      """
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

  @sail-only
  Scenario: EXPLAIN CTAS ORDER BY with multi-partition source shows SortPreservingMergeExec
    Given variable location for temporary directory explain_ctas_order_src
    Given variable location_dst for temporary directory explain_ctas_order_dst
    Given final statement
      """
      DROP TABLE IF EXISTS explain_ctas_order_src_t
      """
    Given final statement
      """
      DROP TABLE IF EXISTS explain_ctas_order_dst_t
      """
    Given statement template
      """
      CREATE TABLE explain_ctas_order_src_t USING PARQUET LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (5), (3), (1), (4), (2) AS t(col)
      """
    Given statement template
      """
      INSERT INTO explain_ctas_order_src_t
      SELECT * FROM VALUES (9), (7), (6), (8), (10) AS t(col)
      """
    When query template
      """
      EXPLAIN CREATE TABLE explain_ctas_order_dst_t
      USING PARQUET LOCATION {{ location_dst.sql }}
      AS SELECT col FROM explain_ctas_order_src_t ORDER BY col ASC
      """
    Then query plan matches snapshot
