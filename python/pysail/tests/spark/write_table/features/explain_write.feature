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
  Scenario: EXPLAIN CREATE TABLE AS SELECT with bucketing shows BucketedParquetSinkExec
    Given variable location for temporary directory explain_bucketed
    Given final statement
      """
      DROP TABLE IF EXISTS explain_bucketed_table
      """
    When query template
      """
      EXPLAIN CREATE TABLE explain_bucketed_table
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave') AS t(id, name)
      """
    Then query plan matches snapshot

  @sail-only
  Scenario: EXPLAIN SELECT from bucketed table shows BucketedParquetScanExec
    Given variable location for temporary directory explain_bucketed_read
    Given final statement
      """
      DROP TABLE IF EXISTS explain_bucketed_read_table
      """
    Given statement template
      """
      CREATE TABLE explain_bucketed_read_table
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave') AS t(id, name)
      """
    When query
      """
      EXPLAIN SELECT * FROM explain_bucketed_read_table
      """
    Then query plan matches snapshot

  @sail-only
  Scenario: EXPLAIN aggregation on bucketed table shows no RepartitionExec
    Given variable location for temporary directory explain_bucketed_agg
    Given final statement
      """
      DROP TABLE IF EXISTS explain_bucketed_agg_table
      """
    Given statement template
      """
      CREATE TABLE explain_bucketed_agg_table
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40) AS t(id, val)
      """
    When query
      """
      EXPLAIN SELECT id, sum(val) FROM explain_bucketed_agg_table GROUP BY id
      """
    Then query plan matches snapshot
