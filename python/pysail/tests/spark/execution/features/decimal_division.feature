Feature: Decimal division in cluster execution
  Scenario Outline: Distributed decimal division uses the coerced input type
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT x / 3 AS result
      FROM (
        SELECT CAST(id AS DECIMAL(5, 2)) AS x FROM range(1, 2)
        UNION ALL
        SELECT CAST(id + 999999999998L AS DECIMAL(20, 2)) AS x FROM range(1, 2)
      )
      """
    Then query result collected
      | result             |
      | 0.333333           |
      | 333333333333.000000 |
    Then query column types
      | column | type          |
      | result | decimal(24,6) |

    Examples:
      | ansi  |
      | true  |
      | false |
