Feature: Projected IN alias filter simplification order

  Scenario Outline: alias null tests simplify negation before Boolean distribution
    When query
      """
      SELECT tag, p
      FROM (
        SELECT tag, <expression> AS p
        FROM VALUES (0, 0, CAST(NULL AS BOOLEAN)), (1, 1, NULL), (2, NULL, NULL) t(tag, v, g)
      ) q
      WHERE p <null_test>
      ORDER BY tag
      """
    Then query result ordered
      | tag | p    |
      | 1   | NULL |

    Examples:
      | expression | null_test |
      | NOT ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) | IS NULL |
      | NOT ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) | IS UNKNOWN |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) = FALSE | IS NULL |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) = FALSE | IS UNKNOWN |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) <> TRUE | IS NULL |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) AND g) <> TRUE | IS UNKNOWN |

  Scenario Outline: alias null tests keep positive existence inside disjunctions
    When query
      """
      SELECT tag, p
      FROM (
        SELECT tag, <expression> AS p
        FROM VALUES (0, 0, CAST(NULL AS BOOLEAN)), (1, 1, NULL), (2, NULL, NULL) t(tag, v, g)
      ) q
      WHERE p <null_test>
      ORDER BY tag
      """
    Then query result ordered
      | tag | p     |
      | 0   | false |
      | 2   | false |

    Examples:
      | expression | null_test |
      | NOT ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) | IS NULL |
      | NOT ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) | IS UNKNOWN |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) = FALSE | IS NULL |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) = FALSE | IS UNKNOWN |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) <> TRUE | IS NULL |
      | ((v IN (SELECT x FROM VALUES (1), (NULL) r(x))) OR g) <> TRUE | IS UNKNOWN |

  Scenario Outline: filtering NULLIF comparisons preserves separate conditional copies
    When query
      """
      SELECT tag, p
      FROM (
        SELECT tag, NULLIF(<conditional>, FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) q
      WHERE p
      ORDER BY tag
      """
    Then query result ordered
      | tag | p    |
      | 0   | NULL |
      | 2   | NULL |

    Examples:
      | conditional |
      | CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) ELSE FALSE END |
      | CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) END |
      | IF(g, v IN (SELECT x FROM VALUES (1), (NULL) r(x)), TRUE) |
      | NULLIF(v IN (SELECT x FROM VALUES (1), (NULL) r(x)), g) |
      | (CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) ELSE FALSE END) = FALSE |

  Scenario Outline: negated NULLIF alias filters reject rows selected only by a null comparison
    When query
      """
      SELECT tag, p
      FROM (
        SELECT tag,
          NULLIF(CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) ELSE FALSE END, FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) q
      WHERE <predicate>
      ORDER BY tag
      """
    Then query result ordered
      | tag | p     |
      | 1   | false |

    Examples:
      | predicate |
      | NOT p |
      | p <=> FALSE |

  Scenario Outline: unused literal null IN branches do not execute their candidate filters
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> AS p
      """
    Then query result
      | p        |
      | <result> |

    Examples:
      | expression | result |
      | FALSE AND (CAST(NULL AS BIGINT) IN (SELECT id FROM range(1) WHERE CAST(CONCAT('bad', id) AS BOOLEAN))) | false |
      | TRUE OR (CAST(NULL AS BIGINT) IN (SELECT id FROM range(1) WHERE CAST(CONCAT('bad', id) AS BOOLEAN))) | true |
      | CASE WHEN FALSE THEN CAST(NULL AS BIGINT) IN (SELECT id FROM range(1) WHERE CAST(CONCAT('bad', id) AS BOOLEAN)) ELSE FALSE END | false |
      | COALESCE(TRUE, CAST(NULL AS BIGINT) IN (SELECT id FROM range(1) WHERE CAST(CONCAT('bad', id) AS BOOLEAN))) | true |

  Scenario Outline: nested conditional comparisons fold equality after one branch push
    When query
      """
      SELECT tag, p FROM (
        SELECT tag, NULLIF(<conditional>, FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) WHERE p
      ORDER BY tag
      """
    Then query result ordered
      | tag | p    |
      | 0   | NULL |
      | 2   | NULL |

    Examples:
      | conditional |
      | (CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) ELSE FALSE END) AND TRUE |
      | NULLIF(v IN (SELECT x FROM VALUES (1), (NULL) r(x)), g) OR FALSE |

  Scenario: filtering nested IF results retains the inner CASE boundary
    When query
      """
      SELECT tag, p FROM (
        SELECT tag, NULLIF(IF(g,
          CASE WHEN g THEN v IN (SELECT x FROM VALUES (1), (NULL) r(x)) ELSE FALSE END,
          TRUE), FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) WHERE NOT p
      ORDER BY tag
      """
    Then query result ordered
      | tag | p     |
      | 1   | false |
      | 3   | false |

  Scenario Outline: filtering complementary NULLIF comparisons preserves alias substitution
    When query
      """
      SELECT COUNT(*) AS n FROM (
        SELECT NULLIF(NULLIF(NVL2(g,
          v IN (SELECT x FROM VALUES (1), (NULL) r(x)), FALSE), TRUE), FALSE) = FALSE AS p
        FROM VALUES (0, TRUE), (1, TRUE), (NULL, TRUE), (0, FALSE) t(v, g)
      ) WHERE <predicate>
      """
    Then query result
      | n   |
      | <n> |

    Examples:
      | predicate | n |
      | p | 3 |
      | NOT p | 0 |

  Scenario: null tests preserve both result copies of complementary NULLIF comparisons
    When query
      """
      SELECT tag, p FROM (
        SELECT tag, NULLIF(NULLIF(NVL2(g,
          v IN (SELECT x FROM VALUES (1), (NULL) r(x)), FALSE), TRUE), FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) WHERE p IS NULL
      ORDER BY tag
      """
    Then query result ordered
      | tag | p    |
      | 1   | NULL |

  Scenario: null tests of NULLIF conjunctions preserve comparison and result copies
    When query
      """
      SELECT tag, p FROM (
        SELECT tag, NULLIF(
          NULLIF(v IN (SELECT x FROM VALUES (1), (NULL) r(x)), g)
            AND v IN (SELECT x FROM VALUES (1), (NULL) r(x)), FALSE) = FALSE AS p
        FROM VALUES (0, 0, TRUE), (1, 1, TRUE), (2, NULL, TRUE), (3, 0, FALSE) t(tag, v, g)
      ) WHERE p IS NULL
      ORDER BY tag
      """
    Then query result ordered
      | tag | p    |
      | 0   | NULL |
      | 1   | NULL |
      | 2   | NULL |
