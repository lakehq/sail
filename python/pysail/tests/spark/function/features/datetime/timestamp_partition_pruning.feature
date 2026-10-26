Feature: Timestamp conversions of string partition columns prune partitions
  A deterministic filter that only references partition columns prunes partitions,
  so a partition directory that does not match the filter is never read.

  Background:
    Given variable location for temporary directory ltz_partition_pruning
    Given variable malformed_location for temporary directory ltz_partition_pruning/p=2021-01-01
    Given final statement
      """
      DROP TABLE IF EXISTS ltz_partition_pruning
      """
    Given final statement
      """
      DROP TABLE IF EXISTS ltz_partition_pruning_malformed
      """
    Given statement template
      """
      CREATE TABLE ltz_partition_pruning (x INT, p STRING)
      USING csv
      OPTIONS (mode 'FAILFAST')
      PARTITIONED BY (p)
      LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO ltz_partition_pruning VALUES (1, '2020-01-01'), (2, '2021-01-01')
      """
    Given statement template
      """
      CREATE TABLE ltz_partition_pruning_malformed (x STRING)
      USING csv
      LOCATION {{ malformed_location.sql }}
      """
    Given statement
      """
      INSERT INTO ltz_partition_pruning_malformed VALUES ('not an int')
      """

  Scenario: Reading the malformed partition fails
    When query
      """
      SELECT x, p FROM ltz_partition_pruning WHERE p = '2021-01-01'
      """
    Then query error .

  Scenario Outline: String partition filter through <name> skips the malformed partition
    When query
      """
      SELECT x, p FROM ltz_partition_pruning
      WHERE <predicate> = TIMESTAMP '2020-01-01 00:00:00'
      """
    Then query result
      | x | p          |
      | 1 | 2020-01-01 |

    Examples:
      | name         | predicate                     |
      | CAST         | CAST(p AS TIMESTAMP)          |
      | to_timestamp | to_timestamp(p)               |
      | formatted    | to_timestamp(p, 'yyyy-MM-dd') |
      | timestamp    | timestamp(p)                  |
      | from_utc     | from_utc_timestamp(p, 'UTC')  |
