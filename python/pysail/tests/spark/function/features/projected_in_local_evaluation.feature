Feature: Early evaluation of local projected IN candidates

  Scenario Outline: unused columns in populated local candidate projections retain eager errors
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused FROM <source>
        ) <suffix>
      ) AS present FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

    Examples:
      | source                                            | suffix      |
      | VALUES (1)                                        |             |
      | (SELECT * FROM VALUES (1) WHERE col1 > 0)           |             |
      | (SELECT * FROM VALUES (1) LIMIT 1)                  |             |
      | VALUES (1)                                        | WHERE false |
      | VALUES (1)                                        | LIMIT 0     |

  Scenario: unused local candidate columns evaluate column-dependent errors
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST(col1 AS INT) AS unused FROM VALUES ('1'), ('invalid')
        )
      ) AS present FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

  Scenario Outline: empty local inputs do not evaluate unused candidate expressions
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused
          FROM (SELECT * FROM VALUES (1) <condition>)
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

    Examples:
      | condition      |
      | WHERE false    |
      | WHERE col1 < 0 |
      | LIMIT 0        |

  Scenario Outline: nonlocal boundaries permit unused candidate expressions to disappear
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused <input>
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

    Examples:
      | input                                        |
      |                                              |
      | FROM range(1)                                |
      | FROM range(0)                                |
      | FROM (SELECT * FROM VALUES (1) LIMIT 1 + 0)    |

  Scenario: local candidate evaluation preserves row-wise Boolean short-circuiting
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, id IN (
        SELECT x FROM (
          SELECT 1 AS x, col1 = 'invalid' OR CAST(col1 AS INT) > 0 AS unused
          FROM VALUES ('invalid'), ('1'), (CAST(NULL AS STRING))
        )
      ) AS present FROM range(2)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 0  | false   |
      | 1  | true    |

  Scenario: materialized local volatile candidates are evaluated once
    When query
      """
      SELECT id, id IN (
        SELECT CAST(value = value AS BIGINT)
        FROM (SELECT rand() AS value FROM VALUES (1), (2))
      ) AS present FROM range(2)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 0  | false   |
      | 1  | true    |

  Scenario Outline: local candidate batches preserve seeded random sequences and lazy siblings
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, id IN (
        SELECT x FROM (
          SELECT CAST(rand(0) * 10 AS BIGINT) AS x, <sibling> AS unused
          FROM VALUES ('invalid'), ('1'), ('2')
        )
      ) AS present FROM range(10)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 0  | true    |
      | 1  | false   |
      | 2  | false   |
      | 3  | false   |
      | 4  | false   |
      | 5  | true    |
      | 6  | false   |
      | 7  | true    |
      | 8  | false   |
      | 9  | false   |

    Examples:
      | sibling                                                                   |
      | true                                                                      |
      | col1 = 'invalid' OR (rand(1) >= 0 AND CAST(col1 AS INT) > 0)                  |

  Scenario: local candidate batches preserve seeded Gaussian random sequences
    When query
      """
      SELECT id, id IN (
        SELECT CAST(randn(0) * 10 AS BIGINT) FROM VALUES (1), (2), (3)
      ) AS present FROM VALUES (-6), (0), (1), (16) AS t(id)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | -6 | true    |
      | 0  | false   |
      | 1  | true    |
      | 16 | true    |

  Scenario: local candidate coalesce skips an unused throwing fallback
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, id IN (
        SELECT COALESCE(col1, CAST('invalid' AS INT)) FROM VALUES (1), (2)
      ) AS present FROM range(3)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 0  | false   |
      | 1  | true    |
      | 2  | true    |

  Scenario: local candidate coalesce evaluates a required throwing fallback
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, COALESCE(col1, CAST('invalid' AS INT)) AS unused
          FROM VALUES (CAST(NULL AS INT))
        )
      ) AS present FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

  Scenario: empty local candidate input skips coalesce evaluation
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, COALESCE(col1, CAST('invalid' AS INT)) AS unused
          FROM (SELECT * FROM VALUES (CAST(NULL AS INT)) WHERE false)
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

  Scenario Outline: local partition expressions preserve eager errors in unused candidate columns
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused, <function>() AS p
          FROM VALUES (1)
        )
      ) AS present FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

    Examples:
      | function                    |
      | spark_partition_id          |
      | monotonically_increasing_id |

  Scenario Outline: empty local partition inputs do not evaluate candidate expressions
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused, <function>() AS p
          FROM (SELECT * FROM VALUES (1) WHERE false)
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

    Examples:
      | function                    |
      | spark_partition_id          |
      | monotonically_increasing_id |

  Scenario Outline: nonlocal partition inputs permit unused candidate columns to disappear
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused, <function>() AS p
          FROM range(1)
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

    Examples:
      | function                    |
      | spark_partition_id          |
      | monotonically_increasing_id |

  Scenario: local candidate partition IDs and monotonic IDs use Spark's partition zero
    When query
      """
      SELECT id, id IN (
        SELECT spark_partition_id() FROM VALUES (10), (20), (30)
      ) AS partition_present, id IN (
        SELECT monotonically_increasing_id() FROM VALUES (10), (20), (30)
      ) AS monotonic_present FROM range(4)
      ORDER BY id
      """
    Then query result ordered
      | id | partition_present | monotonic_present |
      | 0  | true              | true              |
      | 1  | false             | true              |
      | 2  | false             | true              |
      | 3  | false             | false             |

  Scenario Outline: early empty propagation exposes populated local candidate projections
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused FROM (<source>)
        )
      ) AS present FROM range(1)
      """
    Then query error (?i)(cast_invalid_input|cannot cast string)

    Examples:
      | source                                                                                               |
      | SELECT * FROM VALUES (1) UNION ALL SELECT * FROM VALUES (2) WHERE FALSE                               |
      | SELECT * FROM VALUES (2) WHERE FALSE UNION ALL SELECT * FROM VALUES (1)                               |
      | SELECT * FROM VALUES (1) UNION ALL (SELECT * FROM VALUES (2) LIMIT 0)                                  |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r ON TRUE              |
      | SELECT r.col1 FROM (SELECT * FROM VALUES (2) WHERE FALSE) l RIGHT JOIN VALUES (1) r ON TRUE             |
      | SELECT l.col1 FROM VALUES (1) l FULL JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r ON TRUE              |
      | SELECT l.col1 FROM VALUES (1) l LEFT ANTI JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r ON TRUE         |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN VALUES (2) r ON FALSE                                        |
      | SELECT r.col1 FROM VALUES (1) l RIGHT JOIN VALUES (2) r ON FALSE                                       |
      | SELECT l.col1 FROM VALUES (1) l LEFT ANTI JOIN VALUES (2) r ON FALSE                                   |
      | SELECT l.col1 FROM VALUES (1) l LEFT SEMI JOIN VALUES (2) r                                            |

  Scenario Outline: early empty propagation retains nonlocal and empty candidate boundaries
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id IN (
        SELECT x FROM (
          SELECT 1 AS x, CAST('invalid' AS INT) AS unused FROM (<source>)
        )
      ) AS present FROM range(1)
      """
    Then query result
      | present |
      | false   |

    Examples:
      | source                                                                                               |
      | SELECT * FROM VALUES (1) UNION ALL SELECT * FROM VALUES (2)                                           |
      | SELECT * FROM VALUES (1) UNION SELECT * FROM VALUES (2) WHERE FALSE                                   |
      | SELECT l.col1 FROM VALUES (1) l JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r ON TRUE                   |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN VALUES (2) r ON 1 = 2                                        |
      | SELECT l.col1 FROM VALUES (1) l LEFT SEMI JOIN VALUES (2) r ON TRUE                                    |
      | SELECT l.col1 FROM VALUES (1) l LEFT SEMI JOIN VALUES (2) r ON FALSE                                   |
      | SELECT l.col1 FROM VALUES (1) l FULL JOIN VALUES (2) r ON FALSE                                        |
      | SELECT l.col1 FROM VALUES (1) l LEFT ANTI JOIN VALUES (2) r                                            |
      | SELECT * FROM VALUES (1) OFFSET 0                                                                    |
      | SELECT * FROM VALUES (1) LIMIT 1 + 0                                                                  |
      | SELECT * FROM VALUES (1) ORDER BY col1                                                                |
      | SELECT DISTINCT * FROM VALUES (1)                                                                    |

  Scenario Outline: early empty propagation preserves aliases and null padding
    When query
      """
      SELECT id, id IN (SELECT x FROM (<source>)) AS present
      FROM range(4)
      ORDER BY id
      """
    Then query result ordered
      | id | present |
      | 0  | false   |
      | 1  | <one>   |
      | 2  | <two>   |
      | 3  | false   |

    Examples:
      | source                                                                                                                                        | one   | two   |
      | SELECT col1 AS x FROM VALUES (1) WHERE FALSE UNION ALL SELECT col1 AS y FROM VALUES (2)                                                          | false | true  |
      | SELECT COALESCE(l.col1, r.col1) AS x FROM VALUES (1), (2) l FULL JOIN (SELECT * FROM VALUES (3) WHERE FALSE) r ON TRUE                            | true  | true  |
      | SELECT r.col1 AS x FROM VALUES (1) l LEFT JOIN VALUES (2) r ON FALSE                                                                             | false | false |

  Scenario Outline: early local input evaluation preserves the null operand producer
    When query
      """
      SELECT DISTINCT x IN (SELECT 1) AS present
      FROM (SELECT NULLIF(1, 1) AS x FROM (<source>)) t
      """
    Then query result
      | present  |
      | <result> |

    Examples:
      | source                                                                                               | result |
      | SELECT * FROM VALUES (1) UNION ALL SELECT * FROM VALUES (2) WHERE FALSE                               | false  |
      | SELECT * FROM VALUES (2) WHERE FALSE UNION ALL SELECT * FROM VALUES (1)                               | false  |
      | SELECT * FROM VALUES (1) UNION ALL SELECT * FROM VALUES (2)                                           | NULL   |
      | SELECT * FROM VALUES (1) UNION SELECT * FROM VALUES (2) WHERE FALSE                                   | NULL   |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r ON TRUE              | false  |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN VALUES (2) r ON FALSE                                        | false  |
      | SELECT l.col1 FROM VALUES (1) l LEFT JOIN VALUES (2) r ON 1 = 2                                        | NULL   |

  Scenario Outline: conditionless candidate joins retain their requested join type
    When query
      """
      SELECT 1 IN (SELECT x FROM (<source>)) AS present
      """
    Then query result
      | present  |
      | <result> |

    Examples:
      | source                                                                                                   | result |
      | SELECT l.col1 AS x FROM VALUES (1) l LEFT SEMI JOIN VALUES (2), (3) r                                       | true   |
      | SELECT l.col1 AS x FROM VALUES (1) l LEFT ANTI JOIN VALUES (2) r                                            | false  |
      | SELECT l.col1 AS x FROM VALUES (1) l LEFT JOIN (SELECT * FROM VALUES (2) WHERE FALSE) r                      | true   |
      | SELECT r.col1 AS x FROM (SELECT * FROM VALUES (2) WHERE FALSE) l RIGHT JOIN VALUES (1) r                     | true   |
