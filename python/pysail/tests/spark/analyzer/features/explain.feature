Feature: EXPLAIN statement returns Spark-style plan text

  Scenario: Default EXPLAIN returns full physical plan
    When query
      """
      EXPLAIN SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN EXTENDED returns logical and physical plans
    When query
      """
      EXPLAIN EXTENDED SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN CODEGEN shows codegen notice and physical plan
    When query
      """
      EXPLAIN CODEGEN SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN FORMATTED includes statistics and schema
    When query
      """
      EXPLAIN FORMATTED SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN COST shows logical plans with stats
    When query
      """
      EXPLAIN COST SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN ANALYZE executes and returns physical plan
    When query
      """
      EXPLAIN ANALYZE SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot

  Scenario: EXPLAIN VERBOSE returns detailed physical plan
    When query
      """
      EXPLAIN VERBOSE SELECT k, SUM(v) FROM VALUES (1, 2), (1, 3) t(k, v) GROUP BY k
      """
    Then query plan matches snapshot
