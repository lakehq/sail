Feature: Projected IN subquery optimizer boundaries

  Scenario Outline: projected IN distinguishes literal and computed limits over local values
    When query
      """
      SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (
        SELECT id, NULLIF(1, 1) AS x
        FROM (SELECT col1 AS id FROM VALUES (1), (2) LIMIT <limit>) a
      ) producer
      """
    Then query result
      | id | present | absent |
      | 1  | <value> | <value> |

    Examples:
      | limit          | value |
      | 1              | false |
      | 1 + 0          | NULL  |
      | CAST(1 AS INT) | NULL  |
      | 1 OFFSET 0     | NULL  |

  Scenario Outline: projected IN preserves passthrough constants across explode
    When query
      """
      SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (
        SELECT explode(array(1, 2)) AS id, x
        FROM (SELECT CAST(NULL AS INT) AS x FROM <input>) producer
      ) exploded
      ORDER BY id
      """
    Then query result ordered
      | id | present | absent |
      | 1  | <value> | <value> |
      | 2  | <value> | <value> |

    Examples:
      | input      | value |
      | range(1)   | NULL  |
      | VALUES (1) | false |

  Scenario: projected IN preserves passthrough constants across outer explode
    When query
      """
      SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (
        SELECT explode_outer(array()) AS id, x
        FROM (SELECT CAST(NULL AS INT) AS x FROM range(1)) producer
      ) exploded
      """
    Then query result
      | id   | present | absent |
      | NULL | NULL    | NULL   |

  Scenario: projected IN does not propagate an array constant into its exploded elements
    When query
      """
      SELECT id, id IN (SELECT 1) AS present, id NOT IN (SELECT 1) AS absent
      FROM (SELECT explode(array(CAST(NULL AS INT), 1)) AS id FROM range(1)) t
      ORDER BY id NULLS LAST
      """
    Then query result ordered
      | id   | present | absent |
      | 1    | true    | false  |
      | NULL | false   | false  |

  Scenario Outline: projected IN folds filtered union branches independently
    When query
      """
      SELECT tag, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (SELECT 0 AS tag, NULLIF(1, 1) AS x UNION ALL SELECT 1, 1) t
      WHERE <predicate>
      ORDER BY tag
      """
    Then query result ordered
      | tag | present | absent |
      | 0   | NULL    | NULL   |
      | 1   | true    | false  |

    Examples:
      | predicate                         |
      | tag >= 0                          |
      | tag IN (SELECT id FROM range(2))   |

  Scenario: projected IN preserves local values in filtered union branches
    When query
      """
      SELECT tag, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
      FROM (
        SELECT col1 AS tag, NULLIF(1, 1) AS x FROM VALUES (0)
        UNION ALL SELECT 1, 1
      ) t
      WHERE tag >= 0
      ORDER BY tag
      """
    Then query result ordered
      | tag | present | absent |
      | 0   | false   | false  |
      | 1   | true    | false  |

  Scenario Outline: unused volatile columns do not block projected IN union folding
    When query
      """
      SELECT tag, x IN (SELECT 1) AS present
      FROM (
        SELECT tag, x, <unused> AS unused
        FROM (SELECT 0 AS tag, CAST(NULL AS INT) AS x UNION ALL SELECT 1, 1) u
      ) t
      ORDER BY tag
      """
    Then query result ordered
      | tag | present |
      | 0   | NULL    |
      | 1   | true    |

    Examples:
      | unused          |
      | rand(0)         |
      | (SELECT rand(0)) |

  Scenario: used volatile columns keep projected IN above the union
    When query
      """
      SELECT tag, generated >= 0 AS generated, x IN (SELECT 1) AS present
      FROM (
        SELECT tag, x, rand(0) AS generated
        FROM (SELECT 0 AS tag, CAST(NULL AS INT) AS x UNION ALL SELECT 1, 1) u
      ) t
      ORDER BY tag
      """
    Then query result ordered
      | tag | generated | present |
      | 0   | true      | false   |
      | 1   | true      | true    |

  Scenario: a volatile filter keeps projected IN above the union
    When query
      """
      SELECT tag, x IN (SELECT 1) AS present
      FROM (SELECT 0 AS tag, CAST(NULL AS INT) AS x UNION ALL SELECT 1, 1) t
      WHERE rand(0) > 0.5
      ORDER BY tag
      """
    Then query result ordered
      | tag | present |
      | 0   | false   |
      | 1   | true    |

  Scenario Outline: literal null IN does not evaluate an unused nonlocal candidate expression
    When query
      """
      SELECT CAST(NULL AS BIGINT) <operator> (
        SELECT CAST(CONCAT('invalid-', id) AS BIGINT) FROM range(1)
      ) AS present
      """
    Then query result
      | present |
      | NULL    |

    Examples:
      | operator |
      | IN       |
      | NOT IN   |

  Scenario Outline: literal null IN stops after finding a qualifying subquery row
    When query
      """
      SELECT CAST(NULL AS BIGINT) <operator> (
        SELECT id FROM range(20000)
        WHERE CAST(CASE WHEN id < 10000 THEN 'true' ELSE 'invalid' END AS BOOLEAN)
      ) AS present
      """
    Then query result
      | present |
      | NULL    |

    Examples:
      | operator |
      | IN       |
      | NOT IN   |

  Scenario Outline: literal null IN preserves eager subquery evaluation boundaries
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(NULL AS INT) IN (<subquery>) AS present
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

    Examples:
      | subquery                                                                                           |
      | SELECT CAST(x AS INT) FROM VALUES ('invalid') t(x) ORDER BY 1                                         |
      | SELECT CAST(x AS INT) FROM VALUES ('invalid') t(x) LIMIT 1 + 0                                        |
      | SELECT CAST(x AS INT) FROM (SELECT x FROM VALUES ('invalid') t(x) LIMIT 1 + 0) q                       |
      | SELECT MAX(CAST(CASE WHEN id = 0 THEN 'invalid' ELSE '1' END AS INT)) FROM range(2)                     |

  @sail-bug
  Scenario: projected IN folds union branches after a random-bound predicate disappears
    When query
      """
      SELECT tag, x IN (SELECT 1) AS present
      FROM (SELECT 0 AS tag, CAST(NULL AS INT) AS x UNION ALL SELECT 1, 1) t
      WHERE rand(0) >= 0
      ORDER BY tag
      """
    Then query result ordered
      | tag | present |
      | 0   | NULL    |
      | 1   | true    |

  @sail-bug
  Scenario: projected literal null IN prunes a projection above sorted local rows
    # TODO: Match Spark's ordering of eager local evaluation and EXISTS projection pruning.
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(NULL AS INT) IN (
        SELECT CAST(x AS INT)
        FROM (SELECT x FROM VALUES ('invalid') t(x) ORDER BY x) q
      ) AS present
      """
    Then query result
      | present |
      | NULL    |
