Feature: EXPLAIN CODEGEN in distributed execution
  Scenario: EXPLAIN CODEGEN includes the distributed execution plan
    When query
      """
      EXPLAIN CODEGEN
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3) t(k, v)
      GROUP BY k
      """
    Then query plan matches snapshot
