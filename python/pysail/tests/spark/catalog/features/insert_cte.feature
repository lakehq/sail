Feature: CTEs before INSERT statements

  Background:
    Given final statement
      """
      DROP TABLE IF EXISTS insert_cte_target
      """
    Given statement
      """
      CREATE TABLE insert_cte_target (x INT)
      """
    Given statement
      """
      INSERT INTO insert_cte_target VALUES (0)
      """

  Scenario: Insert from CTEs declared before INSERT INTO
    Given statement
      """
      WITH s(x) AS (SELECT 1), t AS (SELECT x + 1 AS x FROM s)
      INSERT INTO insert_cte_target SELECT x FROM t
      """
    When query
      """
      SELECT x FROM insert_cte_target ORDER BY x
      """
    Then query result ordered
      | x |
      | 0 |
      | 2 |

  Scenario: Overwrite from a CTE with an explicit target column
    Given statement
      """
      WITH s AS (SELECT 1 AS x)
      INSERT OVERWRITE TABLE insert_cte_target (x) SELECT x FROM s
      """
    When query
      """
      SELECT x FROM insert_cte_target
      """
    Then query result
      | x |
      | 1 |

  Scenario: Inner INSERT CTEs can reference outer CTEs
    Given statement
      """
      WITH s AS (SELECT 1 AS x)
      INSERT INTO insert_cte_target
      WITH t AS (SELECT x + 1 AS x FROM s) SELECT x FROM t
      """
    When query
      """
      SELECT x FROM insert_cte_target ORDER BY x
      """
    Then query result ordered
      | x |
      | 0 |
      | 2 |

  Scenario: Inner INSERT CTEs shadow outer CTEs
    Given statement
      """
      WITH s AS (SELECT 1 AS x)
      INSERT INTO insert_cte_target
      WITH s AS (SELECT 2 AS x) SELECT x FROM s
      """
    When query
      """
      SELECT x FROM insert_cte_target ORDER BY x
      """
    Then query result ordered
      | x |
      | 0 |
      | 2 |

  Scenario: CTEs after INSERT remain supported
    Given statement
      """
      INSERT INTO insert_cte_target
      WITH s AS (SELECT 1 AS x) SELECT x FROM s
      """
    When query
      """
      SELECT x FROM insert_cte_target ORDER BY x
      """
    Then query result ordered
      | x |
      | 0 |
      | 1 |

  Scenario: Insert by name from a CTE
    Given statement
      """
      WITH s AS (SELECT 1 AS x)
      INSERT INTO insert_cte_target BY NAME SELECT x FROM s
      """
    When query
      """
      SELECT x FROM insert_cte_target ORDER BY x
      """
    Then query result ordered
      | x |
      | 0 |
      | 1 |

  Scenario: Write a directory from a CTE before INSERT
    Given variable output_path for temporary directory insert_cte_directory
    Given statement template
      """
      WITH s AS (SELECT 1 AS x)
      INSERT OVERWRITE DIRECTORY {{ output_path.sql }} USING parquet SELECT x FROM s
      """
    When query template
      """
      SELECT x FROM parquet.`{{ output_path.string }}`
      """
    Then query result
      | x |
      | 1 |
