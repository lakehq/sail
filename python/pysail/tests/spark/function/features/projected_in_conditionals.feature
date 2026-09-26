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

  Scenario Outline: filters retain the comparison inside a projected conditional branch
    When query
      """
      SELECT id, result
      FROM (
        SELECT id, (<conditional>) = FALSE AS result
        FROM VALUES (0), (1), (2), (NULL) t(id)
      ) q
      WHERE result
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | result |
      | 0    | true   |
      | NULL | true   |

    Examples:
      | conditional |
      | IF(id > 0, id IN (SELECT x FROM VALUES (1), (NULL) u(x)), FALSE) |
      | CASE WHEN id > 0 THEN id IN (SELECT x FROM VALUES (1), (NULL) u(x)) ELSE FALSE END |

  Scenario Outline: negated filters preserve projected conditional existence results
    When query
      """
      SELECT id, result
      FROM (
        SELECT id, (<conditional>) = FALSE AS result
        FROM VALUES (0), (1), (2), (NULL) t(id)
      ) q
      WHERE NOT result
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 1  | false  |
      | 2  | false  |

    Examples:
      | conditional |
      | IF(id > 0, id IN (SELECT x FROM VALUES (1), (NULL) u(x)), FALSE) |
      | CASE WHEN id > 0 THEN id IN (SELECT x FROM VALUES (1), (NULL) u(x)) ELSE FALSE END |

  Scenario Outline: NULLIF preserves its comparison and result copies of projected IN
    When query
      """
      SELECT id, result
      FROM (
        SELECT id, <expression> AS result
        FROM VALUES (0), (1), (2), (NULL) t(id)
      ) q
      WHERE <predicate>
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | result |
      | 0    | NULL   |
      | 2    | NULL   |
      | NULL | NULL   |

    Examples:
      | expression | predicate |
      | NULLIF(id IN (SELECT x FROM VALUES (1), (NULL) u(x)), FALSE) | NOT result |
      | NOT NULLIF(id IN (SELECT x FROM VALUES (1), (NULL) u(x)), FALSE) | result |

  Scenario: null-safe comparisons of projected IN aliases retain existence semantics
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, (NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x)))) <=> FALSE AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | true  |
      | 0    | true  |
      | 1    | true  |
      | 2    | true  |

  Scenario: chained comparisons preserve NULLIF filter substitution
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, ((NULLIF(NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x))),FALSE)) = FALSE) = FALSE AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE NOT p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | NULL  |
      | 0    | NULL  |
      | 2    | NULL  |

  Scenario: equal local IN branches fold before a CASE comparison
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, (CASE WHEN id>0 THEN id IN (SELECT x FROM VALUES (1L),(NULL) u(x)) ELSE id IN (SELECT x FROM VALUES (1L),(NULL) u(x)) END) = FALSE AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | false |
      | 0    | false |
      | 1    | false |
      | 2    | false |

  Scenario: complementary local IN conjunctions fold before alias filtering
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, (NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x)))) AND (id IN (SELECT x FROM VALUES (1L),(NULL) u(x))) AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE NOT p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | false |
      | 0    | false |
      | 1    | false |
      | 2    | false |

  Scenario: complementary local IN disjunctions preserve Spark results
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, (NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x)))) OR (id IN (SELECT x FROM VALUES (1L),(NULL) u(x))) AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | true  |
      | 0    | true  |
      | 1    | true  |
      | 2    | true  |

  Scenario: equal local IN branches fold before negative alias filtering
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, CASE WHEN id>0 THEN id IN (SELECT x FROM VALUES (1L),(NULL) u(x)) ELSE id IN (SELECT x FROM VALUES (1L),(NULL) u(x)) END AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE NOT p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |

  Scenario: NULLIF comparison branches simplify before coalesce alias filtering
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, COALESCE((NULLIF(NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x))),id > 0)) = FALSE,TRUE) AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | 0    | true  |
      | 1    | true  |

  Scenario: negated coalesce aliases preserve the copied NULLIF comparison
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, COALESCE((NULLIF(NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x))),id > 0)) = FALSE,TRUE) AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      WHERE NOT p
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | true  |
      | 2    | true  |

  Scenario: NULLIF simplifies its Boolean argument before existence rewriting
    When query
      """
      SELECT id, p
      FROM (
        SELECT id, NULLIF((NOT (id IN (SELECT x FROM VALUES (1L),(NULL) u(x)))) = FALSE,id > 0) AS p
        FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      )
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | false |
      | 0    | NULL  |
      | 1    | NULL  |
      | 2    | false |

  Scenario: different local candidate relations remain distinct during Boolean simplification
    When query
      """
      SELECT id,
        (id IN (SELECT x FROM VALUES (1L), (NULL) u(x)))
        OR NOT (id IN (SELECT x FROM VALUES (2L), (NULL) u(x))) AS p
      FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | false |
      | 0    | false |
      | 1    | true  |
      | 2    | false |

  # Sail only canonicalizes evaluated local candidates. Independently resolved
  # nonlocal plans retain different attribute IDs and miss Spark's complement fold.
  @sail-bug
  Scenario Outline: equivalent nonlocal projected IN complements fold together
    When query
      """
      SELECT id, (id IN (<source>)) OR NOT (id IN (<source>)) AS p
      FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      ORDER BY id
      """
    Then query result ordered
      | id   | p    |
      | NULL | true |
      | 0    | true |
      | 1    | true |
      | 2    | true |

    Examples:
      | source                       |
      | SELECT id FROM range(2)      |
      | SELECT MAX(id) FROM range(2) |

  Scenario: NULLIF with a null second argument disappears before projected IN negation
    When query
      """
      SELECT id,
        NOT NULLIF(id IN (SELECT x FROM VALUES (1L), (NULL) u(x)), NULL) AS p
      FROM VALUES (0L), (1L), (2L), (NULL) t(id)
      ORDER BY id
      """
    Then query result ordered
      | id   | p     |
      | NULL | false |
      | 0    | false |
      | 1    | false |
      | 2    | false |

  Scenario Outline: local IN relation equality follows Spark floating point row equality
    When query
      """
      SELECT (value IN (SELECT x FROM VALUES (<first>), (CAST(NULL AS <type>)) a(x)))
        OR NOT (value IN (SELECT x FROM VALUES (<second>), (CAST(NULL AS <type>)) b(x))) AS p
      FROM VALUES (CAST(NULL AS <type>)) t(value)
      """
    Then query result
      | p    |
      | true |

    Examples:
      | type | first | second |
      | FLOAT | CAST('0.0' AS FLOAT) | -CAST('0.0' AS FLOAT) |
      | FLOAT | CAST('NaN' AS FLOAT) | -CAST('NaN' AS FLOAT) |
      | ARRAY<FLOAT> | array(CAST('0.0' AS FLOAT)) | array(-CAST('0.0' AS FLOAT)) |
      | ARRAY<FLOAT> | array(CAST('NaN' AS FLOAT)) | array(-CAST('NaN' AS FLOAT)) |
      | STRUCT<x:FLOAT> | named_struct('x', CAST('0.0' AS FLOAT)) | named_struct('x', -CAST('0.0' AS FLOAT)) |
      | STRUCT<x:FLOAT> | named_struct('x', CAST('NaN' AS FLOAT)) | named_struct('x', -CAST('NaN' AS FLOAT)) |
      | DOUBLE | CAST('0.0' AS DOUBLE) | -CAST('0.0' AS DOUBLE) |
      | DOUBLE | CAST('NaN' AS DOUBLE) | -CAST('NaN' AS DOUBLE) |
      | ARRAY<DOUBLE> | array(CAST('0.0' AS DOUBLE)) | array(-CAST('0.0' AS DOUBLE)) |
      | ARRAY<DOUBLE> | array(CAST('NaN' AS DOUBLE)) | array(-CAST('NaN' AS DOUBLE)) |
      | STRUCT<x:DOUBLE> | named_struct('x', CAST('0.0' AS DOUBLE)) | named_struct('x', -CAST('0.0' AS DOUBLE)) |
      | STRUCT<x:DOUBLE> | named_struct('x', CAST('NaN' AS DOUBLE)) | named_struct('x', -CAST('NaN' AS DOUBLE)) |
