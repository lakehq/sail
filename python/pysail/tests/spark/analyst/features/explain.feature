Feature: EXPLAIN statement returns Spark-style plan text

  Scenario: Default EXPLAIN returns full physical plan
    When query
      """
      SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN EXTENDED returns logical and physical plans
    When query
      """
      SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then extended query plan matches snapshot

  Scenario: EXPLAIN CODEGEN shows codegen notice and physical plan
    When query
      """
      SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then codegen query plan matches snapshot

  Scenario: EXPLAIN FORMATTED includes statistics and schema
    When query
      """
      SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then formatted query plan matches snapshot

  Scenario: EXPLAIN COST shows logical plans with stats
    When query
      """
      SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then cost query plan matches snapshot
