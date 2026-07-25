Feature: skewness and kurtosis over window functions

  Rule: skewness over expanding window

    Scenario: skewness returns NULL for single value and computes for two or more
      When query
      """
      SELECT
        id,
        value,
        CAST(skewness(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS skew
      FROM VALUES (1, 10), (2, 20), (3, 30), (4, 100), (5, 50) AS t(id, value)
      """
      Then query result ordered
      | id | value | skew |
      | 1  | 10    | NULL |
      | 2  | 20    | 0.00 |
      | 3  | 30    | 0.00 |
      | 4  | 100   | 1.02 |
      | 5  | 50    | 0.93 |

  Rule: kurtosis over expanding window

    Scenario: kurtosis returns NULL for single value and computes for two or more
      When query
      """
      SELECT
        id,
        value,
        CAST(kurtosis(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS kurt
      FROM VALUES (1, 10), (2, 20), (3, 30), (4, 100), (5, 50) AS t(id, value)
      """
      Then query result ordered
      | id | value | kurt  |
      | 1  | 10    | NULL  |
      | 2  | 20    | -2.00 |
      | 3  | 30    | -1.50 |
      | 4  | 100   | -0.77 |
      | 5  | 50    | -0.55 |
