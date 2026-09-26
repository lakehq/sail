Feature: Short-circuit projected IN expressions

  Scenario Outline: constant Boolean branches discard an unreachable IN subquery before validation
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        NOT (false AND (id IN (SELECT 1))) AS conjunction,
        NOT (true OR (id IN (SELECT 1))) AS disjunction,
        (false AND (id IN (SELECT 1))) = false AS comparison,
        NOT (false AND (id IN (SELECT CAST(NULL AS BIGINT)))) AS null_candidate,
        NOT (false AND (id IN (SELECT id FROM range(1) WHERE false))) AS empty_candidate
      FROM range(3)
      ORDER BY id
      """
    Then query result ordered
      | id | conjunction | disjunction | comparison | null_candidate | empty_candidate |
      | 0  | true        | false       | true       | true           | true            |
      | 1  | true        | false       | true       | true           | true            |
      | 2  | true        | false       | true       | true           | true            |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: conditional branches discard an unreachable IN subquery before validation
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id,
        (CASE WHEN false THEN id IN (SELECT 1) ELSE false END) = false AS conditional,
        NOT IF(false, id IN (SELECT 1), false) AS if_result,
        NOT COALESCE(true, id IN (SELECT 1)) AS coalesce_result
      FROM range(3)
      ORDER BY id
      """
    Then query result ordered
      | id | conditional | if_result | coalesce_result |
      | 0  | true        | true      | false           |
      | 1  | true        | true      | false           |
      | 2  | true        | true      | false           |

    Examples:
      | ansi  |
      | true  |
      | false |
