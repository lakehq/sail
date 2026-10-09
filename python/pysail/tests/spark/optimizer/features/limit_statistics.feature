Feature: Limit cardinality during physical requirement enforcement

  Scenario: A small limited input avoids extra aggregate repartitioning
    When query
      """
      SELECT id % 2 AS k, count(*) AS n
      FROM (SELECT id FROM range(100000) LIMIT 2)
      GROUP BY id % 2 ORDER BY k
      """
    Then query result
      | k | n |
      | 0 | 1 |
      | 1 | 1 |
    When query
      """
      EXPLAIN SELECT id % 2 AS k, count(*) AS n
      FROM (SELECT id FROM range(100000) LIMIT 2)
      GROUP BY id % 2 ORDER BY k
      """
    Then query plan matches snapshot

  Scenario: Limited scalar aggregates avoid extra join repartitioning
    When query
      """
      SELECT a.n AS na, b.n AS nb
      FROM (SELECT count(*) AS n FROM range(100000) WHERE id % 2 = 0 LIMIT 100) a
      CROSS JOIN (SELECT count(*) AS n FROM range(100000) WHERE id % 2 = 0 LIMIT 100) b
      """
    Then query result
      | na     | nb     |
      | 50000  | 50000  |
    When query
      """
      EXPLAIN SELECT a.n AS na, b.n AS nb
      FROM (SELECT count(*) AS n FROM range(100000) WHERE id % 2 = 0 LIMIT 100) a
      CROSS JOIN (SELECT count(*) AS n FROM range(100000) WHERE id % 2 = 0 LIMIT 100) b
      """
    Then query plan matches snapshot
