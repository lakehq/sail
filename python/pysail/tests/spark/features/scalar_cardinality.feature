@spark-4
Feature: Correlated scalar cardinality checks
  Background:
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW scalar_cardinality_inner AS
      SELECT * FROM VALUES (1, 10), (2, 20), (2, 21), (4, CAST(NULL AS INT)) AS t(k, v)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS scalar_cardinality_inner
      """

  Scenario: Scalar cardinality ignores groups without matching outer rows
    When query
      """
      SELECT o.k, (SELECT i.v FROM scalar_cardinality_inner i WHERE i.k = o.k) AS v
      FROM VALUES (1), (1), (3), (4), (CAST(NULL AS INT)) o(k)
      """
    Then query result collected
      | k    | v    |
      | 1    | 10   |
      | 1    | 10   |
      | 3    | NULL |
      | 4    | NULL |
      | NULL | NULL |
    Then query column types
      | column | type |
      | k      | int  |
      | v      | int  |

  Scenario: Scalar cardinality stays above correlation when used in a filter
    When query
      """
      SELECT o.k FROM VALUES (1), (3), (4) o(k)
      WHERE (SELECT i.v FROM scalar_cardinality_inner i WHERE i.k = o.k) > 0
      """
    Then query result collected
      | k |
      | 1 |

  Scenario: Scalar cardinality preserves nested values and missing rows
    When query
      """
      SELECT o.k,
        (SELECT named_struct('v', array(i.v)) FROM scalar_cardinality_inner i WHERE i.k = o.k) AS s
      FROM VALUES (1), (3), (4) o(k)
      """
    Then query result collected
      | k | s             |
      | 1 | Row(v=[10])   |
      | 3 | NULL          |
      | 4 | Row(v=[None]) |

  Scenario: Scalar cardinality rejects multiple matching rows
    When query
      """
      SELECT o.k, (SELECT i.v FROM scalar_cardinality_inner i WHERE i.k = o.k) AS v
      FROM VALUES (1), (2), (3) o(k)
      """
    Then query error (?i)(more than one row|too_many_rows)

  Scenario: Scalar cardinality can be checked in a grouped projection
    When query
      """
      SELECT o.k, (SELECT i.v FROM scalar_cardinality_inner i WHERE i.k = o.k) AS v
      FROM VALUES (1), (1), (3), (4) o(k)
      GROUP BY o.k
      """
    Then query result collected
      | k | v    |
      | 1 | 10   |
      | 3 | NULL |
      | 4 | NULL |

  Scenario: Scalar cardinality counts null values as rows
    When query
      """
      SELECT (SELECT i.v FROM VALUES (1, CAST(NULL AS INT)), (1, NULL) i(k, v) WHERE i.k = o.k) AS v
      FROM VALUES (1) o(k)
      """
    Then query error (?i)(more than one row|too_many_rows)
