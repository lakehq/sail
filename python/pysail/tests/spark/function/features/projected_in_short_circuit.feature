Feature: Short-circuit projected IN expressions

  Scenario Outline: unreachable projected IN preserves eager constant subquery errors
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> AS result FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

    Examples:
      | expression                                                               |
      | false AND (id IN (SELECT CAST('invalid' AS INT)))                          |
      | true OR (id IN (SELECT CAST('invalid' AS INT)))                            |
      | CASE WHEN false THEN NULL IN (SELECT CAST('invalid' AS INT)) ELSE true END |
      | COALESCE(true, id IN (SELECT CAST('invalid' AS INT)))                       |

  Scenario: unreachable projected IN does not execute a runtime candidate expression
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT false AND (id IN (
        SELECT CAST(CONCAT('invalid-', id) AS BIGINT) FROM range(1)
      )) AS result FROM range(1)
      """
    Then query result
      | result |
      | false  |

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
