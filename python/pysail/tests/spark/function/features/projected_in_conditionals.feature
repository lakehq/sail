Feature: Conditional boundaries around projected IN

  Scenario Outline: conditional negation preserves Spark existence semantics
    When query
      """
      SELECT id, <expression> AS result
      FROM VALUES (1), (2), (NULL) u(id)
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | result |
      | 1    | <one>  |
      | 2    | <two>  |
      | NULL | <null> |

    Examples:
      | expression                                                                                                           | one   | two   | null  |
      | NOT (CASE WHEN id = 2 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END)                               | true  | true  | true  |
      | NOT COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                      | false | true  | true  |
      | COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) = FALSE                                                   | false | true  | true  |
      | NOT COALESCE(CASE WHEN id = 2 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END, FALSE)                | true  | true  | true  |
      | NOT (CASE id WHEN 2 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END)                                  | true  | true  | true  |
      | NOT (CASE WHEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) THEN TRUE ELSE FALSE END)                                 | false | true  | true  |
      | NOT (CASE WHEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) THEN FALSE ELSE TRUE END)                                 | true  | false | false |
      | NOT (CASE WHEN id = 2 THEN id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END)                           | true  | true  | true  |
      | NOT COALESCE(id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                  | true  | true  | true  |
      | COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) IS NULL                                                   | false | false | false |
      | NOT (CASE WHEN TRUE THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END)                                 | false | false | false |
      | NOT COALESCE(CAST(NULL AS BOOLEAN), id IN (SELECT x FROM VALUES (1), (NULL) t(x)))                                      | false | false | false |
      | NOT COALESCE(FALSE, id IN (SELECT x FROM VALUES (1), (NULL) t(x)))                                                     | true  | true  | true  |
      | NOT NVL(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                          | false | true  | true  |
      | NOT IFNULL(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                       | false | true  | true  |
      | NVL(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) = FALSE                                                      | false | true  | true  |
      | IFNULL(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) = FALSE                                                   | false | true  | true  |
      | NOT NVL(CAST(NULL AS BOOLEAN), id IN (SELECT x FROM VALUES (1), (NULL) t(x)))                                          | false | false | false |
      | NOT IFNULL(FALSE, id IN (SELECT x FROM VALUES (1), (NULL) t(x)))                                                       | true  | true  | true  |
      | NOT NVL2(id > 0, id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                 | false | true  | true  |
      | NVL2(id > 0, id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) = FALSE                                             | false | false | true  |
      | (CASE WHEN id > 0 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END) = FALSE                          | false | false | true  |
      | FALSE = (CASE WHEN id > 0 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END)                          | false | false | true  |
      | NOT NVL2(CAST(NULL AS BOOLEAN), FALSE, id IN (SELECT x FROM VALUES (1), (NULL) t(x)))                                 | false | false | false |
      | NOT NVL2(TRUE, id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)                                                   | false | false | false |
      | NOT NVL(CASE WHEN id = 2 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x)) ELSE FALSE END, FALSE)                     | true  | true  | true  |
      | NOT (CASE WHEN id = 2 THEN IFNULL(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) ELSE FALSE END)                | true  | true  | true  |

  Scenario: negation distinguishes conditional and direct IN within one expression
    When query
      """
      SELECT id,
        NOT (
          COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)
          OR id IN (SELECT x FROM VALUES (1), (NULL) t(x))
        ) AS result
      FROM VALUES (1), (2), (NULL) u(id)
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | result |
      | 1    | false  |
      | 2    | false  |
      | NULL | false  |

  Scenario: several CASE branches retain positive existence results
    When query
      """
      SELECT id,
        NOT (CASE
          WHEN id = 1 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
          WHEN id = 2 THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
          WHEN id IS NULL THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
          ELSE FALSE END) AS result
      FROM VALUES (1), (2), (NULL) u(id)
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | result |
      | 1    | false  |
      | 2    | true   |
      | NULL | true   |

  Scenario: filtering a conditional alias preserves its existence boundary
    When query
      """
      SELECT id FROM (
        SELECT id,
          NOT COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) AS result
        FROM VALUES (1), (2), (NULL) u(id)
      ) q
      WHERE result
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   |
      | 2    |
      | NULL |

  Scenario: a nested subquery preserves its conditional existence result
    When query
      """
      SELECT TRUE IN (
        SELECT NOT COALESCE(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)
        FROM VALUES (1), (2), (NULL) u(id)
      ) AS result
      """
    Then query result
      | result |
      | true   |

  Scenario Outline: nested subqueries preserve NVL and IFNULL existence boundaries
    When query
      """
      SELECT TRUE IN (
        SELECT NOT <function>(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE)
        FROM VALUES (1), (2), (NULL) u(id)
      ) AS result
      """
    Then query result
      | result |
      | true   |

    Examples:
      | function |
      | NVL      |
      | IFNULL   |

  Scenario Outline: filtering an NVL or IFNULL alias preserves its existence boundary
    When query
      """
      SELECT id FROM (
        SELECT id,
          NOT <function>(id IN (SELECT x FROM VALUES (1), (NULL) t(x)), FALSE) AS result
        FROM VALUES (1), (2), (NULL) u(id)
      ) q WHERE result
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   |
      | 2    |
      | NULL |

    Examples:
      | function |
      | NVL      |
      | IFNULL   |

  Scenario: unreachable conditional branches discard outer expression errors
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id,
        CASE WHEN FALSE
          THEN CAST('invalid' AS INT) = 1 AND id IN (SELECT 1)
          ELSE TRUE END AS case_result,
        COALESCE(TRUE, CAST('invalid' AS BOOLEAN) AND id IN (SELECT 1)) AS coalesce_result
      FROM range(3)
      ORDER BY id
      """
    Then query result ordered
      | id | case_result | coalesce_result |
      | 0  | true        | true            |
      | 1  | true        | true            |
      | 2  | true        | true            |

  # Spark defers a failed fold until its data-dependent branch is evaluated.
  # Sail's literal-cast simplifier still raises the unreachable ELSE error.
  @sail-bug
  Scenario: data-dependent CASE defers an unreachable literal cast error
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CASE WHEN id = 0
        THEN id IN (SELECT 1)
        ELSE CAST('invalid' AS BOOLEAN) END AS result
      FROM range(1)
      """
    Then query result
      | result |
      | false  |

  # The existence result is nonnullable, so Spark discards this COALESCE suffix.
  # Sail folds the invalid literal cast before eliminating the suffix.
  @sail-bug
  Scenario: COALESCE defers a literal cast error after projected existence
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT COALESCE(
        id IN (SELECT x FROM VALUES (1), (NULL) t(x)),
        CAST('invalid' AS BOOLEAN)
      ) AS result
      FROM VALUES (0) u(id)
      """
    Then query result
      | result |
      | false  |
