Feature: count_if function

  Rule: count_if as window function

    Scenario: count_if over window
      When query
      """
      SELECT value,
             count_if(value > 2) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
      FROM VALUES (1), (2), (3), (4), (5) AS t(value)
      ORDER BY value
      """
      Then query result ordered
      | value | result |
      | 1     | 0      |
      | 2     | 0      |
      | 3     | 1      |
      | 4     | 2      |
      | 5     | 3      |
