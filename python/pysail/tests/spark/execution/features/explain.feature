Feature: Distributed EXPLAIN in distributed execution
  Scenario: Distributed EXPLAIN includes the execution stage graph
    When query
      """
      EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT, VERBOSE TRUE)
      SELECT k, SUM(v) AS total
      FROM VALUES (1, 2), (1, 3) t(k, v)
      GROUP BY k
      """
    Then query plan matches snapshot
