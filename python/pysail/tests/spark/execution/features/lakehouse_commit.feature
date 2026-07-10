Feature: Lakehouse commits in distributed execution

  Scenario: Delta commit runs on the driver after parallel file writing
    Given variable location for temporary directory distributed_delta_commit
    Given final statement
      """
      DROP TABLE IF EXISTS distributed_delta_commit
      """
    Given statement template
      """
      CREATE TABLE distributed_delta_commit (id BIGINT)
      USING delta
      LOCATION {{ location.uri }}
      """
    When query
      """
      EXPLAIN
      INSERT INTO distributed_delta_commit
      SELECT id FROM range(0, 400, 1, 4)
      """
    Then query plan matches snapshot

  Scenario: Iceberg commit runs on the driver after parallel file writing
    Given variable location for temporary directory distributed_iceberg_commit
    Given final statement
      """
      DROP TABLE IF EXISTS distributed_iceberg_commit
      """
    Given statement template
      """
      CREATE TABLE distributed_iceberg_commit (id BIGINT)
      USING iceberg
      LOCATION {{ location.uri }}
      """
    When query
      """
      EXPLAIN
      INSERT INTO distributed_iceberg_commit
      SELECT id FROM range(0, 400, 1, 4)
      """
    Then query plan matches snapshot
