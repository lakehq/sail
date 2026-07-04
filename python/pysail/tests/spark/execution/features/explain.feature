Feature: EXPLAIN in distributed execution
  Scenario: EXPLAIN includes the distributed execution plan in local-cluster mode
    When query
      """
      EXPLAIN
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3) t(k, v)
      GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN includes the scalar subquery distributed plan in local-cluster mode
    When query
      """
      EXPLAIN
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3), (2, 10), (3, 1) AS t(k, v)
      WHERE v > (SELECT MAX(x) FROM VALUES (1), (2) AS s(x))
      GROUP BY k
      """
    Then query plan matches snapshot
