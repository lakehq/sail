@sail-only
Feature: Delta Lake OPTIMIZE

  Background:
    Given variable location for temporary directory delta_optimize
    Given final statement
      """
      DROP TABLE IF EXISTS delta_optimize
      """
    Given statement template
      """
      CREATE TABLE delta_optimize (id INT, value STRING)
      USING DELTA LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_optimize VALUES (1, 'a')
      """
    Given statement
      """
      INSERT INTO delta_optimize VALUES (2, 'b')
      """
    Given statement
      """
      INSERT INTO delta_optimize VALUES (3, 'c')
      """

  Scenario: OPTIMIZE compacts files without changing rows
    Then delta active data files count is 3
    Given statement
      """
      OPTIMIZE delta_optimize
      """
    Then delta active data files count is 1
    When query
      """
      SELECT * FROM delta_optimize ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 1  | a     |
      | 2  | b     |
      | 3  | c     |
    Then delta log latest commit info contains
      | path                             | value      |
      | operation                        | "OPTIMIZE" |
      | operationMetrics.numAddedFiles   | 1          |
      | operationMetrics.numRemovedFiles | 3          |
    Then delta log latest commit rewrites files without data changes
    Given statement
      """
      OPTIMIZE delta_optimize
      """
    Then delta active data files count is 1
    Then delta log commit count is 5

  Scenario: OPTIMIZE compacts each partition independently
    Given variable location for temporary directory delta_optimize_partitioned
    Given final statement
      """
      DROP TABLE IF EXISTS delta_optimize_partitioned
      """
    Given statement template
      """
      CREATE TABLE delta_optimize_partitioned (id INT, bucket INT)
      USING DELTA PARTITIONED BY (bucket) LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_optimize_partitioned VALUES (1, 0), (2, 1)
      """
    Given statement
      """
      INSERT INTO delta_optimize_partitioned VALUES (3, 0), (4, 1)
      """
    Given statement
      """
      OPTIMIZE delta_optimize_partitioned
      """
    Then delta active data files count is 2
    When query
      """
      SELECT * FROM delta_optimize_partitioned ORDER BY id
      """
    Then query result ordered
      | id | bucket |
      | 1  | 0      |
      | 2  | 1      |
      | 3  | 0      |
      | 4  | 1      |
