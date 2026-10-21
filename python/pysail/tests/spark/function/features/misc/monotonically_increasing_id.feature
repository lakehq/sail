Feature: monotonically_increasing_id evaluation

  Scenario: conditional filters advance monotonically increasing IDs only for evaluated rows
    When query
      """
      SELECT id
      FROM range(0, 10, 1, 1)
      WHERE CASE WHEN id % 2 = 0 THEN monotonically_increasing_id() ELSE 100 END < 3
      """
    Then query result
      | id |
      | 0  |
      | 2  |
      | 4  |

  Scenario: conditional HAVING calls evaluate after aggregation
    When query
      """
      SELECT id % 2 AS k, count(*) AS n
      FROM range(0, 40, 1, 1)
      GROUP BY id % 2
      HAVING CASE WHEN k = 0 THEN monotonically_increasing_id() % 8589934592 ELSE 100 END < 1
      """
    Then query result
      | k | n  |
      | 0 | 20 |
