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

    Scenario: count_if over window with RESPECT NULLS
      When query
      """
      SELECT id,
             flag,
             count_if(flag) RESPECT NULLS OVER (
               ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
             ) AS result
      FROM VALUES
        (1, CAST(NULL AS BOOLEAN)),
        (2, true),
        (3, false),
        (4, CAST(NULL AS BOOLEAN)),
        (5, true)
        AS t(id, flag)
      ORDER BY id
      """
      Then query result ordered
      | id | flag  | result |
      | 1  | NULL  | 0      |
      | 2  | true  | 1      |
      | 3  | false | 1      |
      | 4  | NULL  | 1      |
      | 5  | true  | 1      |
