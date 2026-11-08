Feature: Division by zero behavior

  Rule: All division by zero returns NULL when ANSI mode is disabled (Spark 4.x behavior)
    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                         | expr                                                    |
        | Float divided by zero returns NULL           | 1.0 / 0.0                                               |
        | Negative float divided by zero returns NULL  | -1.0 / 0.0                                              |
        | Zero divided by zero returns NULL            | 0.0 / 0.0                                               |
        | Integer divided by integer zero returns NULL | 1 / 0                                                   |
        | Integer divided by float zero returns NULL   | 1 / 0.0                                                 |
        | Decimal divided by decimal zero returns NULL | CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) |
        | Decimal divided by integer zero returns NULL | CAST(100.50 AS DECIMAL(10,2)) / 0                       |

  Rule: Division by zero throws error when ANSI mode is enabled
    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error <error>

      Examples:
        | case                                                             | expr                                                    | error                |
        | Integer divided by zero throws error in ANSI mode                | 1 / 0                                                   | (?i)divide.*zero     |
        | Float divided by zero throws error in ANSI mode                  | 1.0 / 0.0                                               | (?i)divide.*zero     |
        | Decimal divided by decimal zero throws error in ANSI mode        | CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) | (?i)divide.*zero     |
        | Decimal divided by integer zero throws error in ANSI mode        | CAST(100.50 AS DECIMAL(10,2)) / 0                       | (?i)divide.*zero     |
        | DIV by literal zero throws error in ANSI mode                    | 10 DIV 0                                                | (?i)divide.*zero     |
        | Modulo by literal zero throws error in ANSI mode                 | 10 % 0                                                  | (?i)remainder.*zero  |
        | Computed expression evaluating to zero throws error in ANSI mode | 1 / (1 - 1)                                             | (?i)division by zero |

  Rule: Dynamic divisor division by zero raises error in ANSI mode
    Scenario Outline: Dynamic divisor: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result FROM <from>
        """
      Then query error <error>

      Examples:
        | case                                                      | expr        | from                  | error                |
        | Integer divided by dynamic zero raises error in ANSI mode | 10 / id     | (VALUES (0)) AS t(id) | (?i)division by zero |
        | DIV by dynamic zero raises error in ANSI mode             | 10 DIV id   | (VALUES (0)) AS t(id) | (?i)division by zero |
        | Modulo by dynamic zero raises error in ANSI mode          | 10 % id     | (VALUES (0)) AS t(id) | (?i)remainder.*zero  |
        | mod function with dynamic zero raises error in ANSI mode  | mod(10, id) | (VALUES (0)) AS t(id) | (?i)remainder.*zero  |
        | Division by zero in range raises error in ANSI mode       | 1 / id      | range(2)              | (?i)division by zero |

    Scenario Outline: Dynamic divisor (typed): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)division by zero

      Examples:
        | case                                                      | expr                                                    |
        | Decimal divided by dynamic zero raises error in ANSI mode | CAST(10.5 AS DECIMAL(10,2)) / CAST(id AS DECIMAL(10,2)) |
        | Double divided by dynamic zero raises error in ANSI mode  | CAST(1 AS DOUBLE) / CAST(id AS DOUBLE)                  |

    Scenario Outline: Two-column: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a, b, a <op> b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query error <error>

      Examples:
        | case                                                            | op | error                |
        | Two-column division with zero divisor raises error in ANSI mode | /  | (?i)division by zero |
        | Two-column modulo with zero divisor raises error in ANSI mode   | %  | (?i)remainder.*zero  |

  Rule: Dynamic divisor division by zero returns NULL in non-ANSI mode
    Scenario Outline: Dynamic divisor non-ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                          | expr      |
        | Integer divided by dynamic zero returns NULL in non-ANSI mode | 10 / id   |
        | DIV by dynamic zero returns NULL in non-ANSI mode             | 10 DIV id |
        | Modulo by dynamic zero returns NULL in non-ANSI mode          | 10 % id   |

    Scenario: Decimal divided by dynamic zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) / CAST(id AS DECIMAL(10,2)) AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

  Rule: DIV and modulo by literal zero returns NULL in non-ANSI mode
    Scenario Outline: Literal zero: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                                 | expr        |
        | DIV by literal zero returns NULL in non-ANSI mode                    | 10 DIV 0    |
        | Modulo by literal zero returns NULL in non-ANSI mode                 | 10 % 0      |
        | Computed expression evaluating to zero returns NULL in non-ANSI mode | 1 / (1 - 1) |

    Scenario: Division by zero in range returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / id AS result FROM range(2)
        """
      Then query result
        | result |
        | NULL   |
        | 1.0    |

    Scenario: Two-column division with zero divisor returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a, b, a / b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query result
        | a  | b  | result             |
        | -2 | -1 | 2.0                |
        | -1 | 0  | NULL               |
        | 0  | 1  | 0.0                |
        | 1  | 2  | 0.5                |
        | 2  | 3  | 0.6666666666666666 |

    Scenario: Two-column modulo with zero divisor returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a, b, a % b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query result
        | a  | b  | result |
        | -2 | -1 | 0      |
        | -1 | 0  | NULL   |
        | 0  | 1  | 0      |
        | 1  | 2  | 1      |
        | 2  | 3  | 2      |

  Rule: Non-zero dynamic divisors work normally
    Scenario: Integer divided by non-zero dynamic divisor works in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 / id AS result FROM (VALUES (2)) AS t(id)
        """
      Then query result
        | result |
        | 5.0    |

  Rule: A literal zero divisor only raises an error for the rows that evaluate the division
    Scenario Outline: ANSI on, the division is not evaluated: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, <expr> AS result
        FROM VALUES (0, 5, 5.0D), (1, 7, 7.0D) AS t(id, i, d)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 0  | <result_0> |
        | 1  | <result_1> |

      Examples:
        | case                     | expr                                            | result_0 | result_1 |
        | coalesce with div        | coalesce(i, 1 div 0)                            | 5        | 7        |
        | coalesce with divide     | coalesce(d, 1 / 0)                              | 5.0      | 7.0      |
        | coalesce with modulo     | coalesce(i, 1 % 0)                              | 5        | 7        |
        | nvl with column dividend | nvl(i, i div 0)                                 | 5        | 7        |
        | CASE ELSE branch         | CASE WHEN i IS NOT NULL THEN i ELSE 1 div 0 END | 5        | 7        |
        | if false branch          | if(i > 0, i, 1 div 0)                           | 5        | 7        |
        | try_divide alternative   | coalesce(d, try_divide(1, 0))                   | 5.0      | 7.0      |

    Scenario Outline: ANSI on, a row evaluates the division: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, <expr> AS result
        FROM VALUES (0, 5), (1, CAST(NULL AS INT)) AS t(id, i)
        ORDER BY id
        """
      Then query error <error>

      Examples:
        | case                 | expr                 | error             |
        | coalesce with div    | coalesce(i, 1 div 0) | DIVIDE_BY_ZERO    |
        | coalesce with divide | coalesce(i, 1 / 0)   | DIVIDE_BY_ZERO    |
        | coalesce with modulo | coalesce(i, 1 % 0)   | REMAINDER_BY_ZERO |

    Scenario Outline: ANSI on, a literal division by zero raises the Spark error: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error <error>

      Examples:
        | case   | expr      | error             |
        | divide | 1 / 0     | DIVIDE_BY_ZERO    |
        | div    | 1 div 0   | DIVIDE_BY_ZERO    |
        | modulo | 1 % 0     | REMAINDER_BY_ZERO |
        | mod    | mod(1, 0) | REMAINDER_BY_ZERO |

    Scenario: ANSI on, a filter that is always false does not evaluate the division
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id FROM range(3) WHERE false AND 1 div 0 = 1
        """
      Then query result
        | id |

    # Spark folds constant expressions outside of conditional expressions when the query is planned,
    # so it raises the error even if no row is evaluated. This is an optimizer artifact.
    @sail-only
    Scenario: ANSI on, a division by zero in a query without rows is not evaluated
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / 0 AS result FROM range(0)
        """
      Then query result
        | result |

    Scenario Outline: ANSI off, a literal division by zero has the type of the division: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result, typeof(<expr>) AS result_type
        """
      Then query result
        | result | result_type |
        | NULL   | <type>      |

      Examples:
        | case            | expr      | type   |
        | integer divide  | 1 / 0     | double |
        | double divide   | 1.5D / 0  | double |
        | div             | 1 div 0   | bigint |
        | integer modulo  | 1 % 0     | int    |
        | double modulo   | 1.5D % 0D | double |
        | bigint div      | 10L div 0 | bigint |

    Scenario: ANSI off, coalesce with a literal division by zero
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, coalesce(i, 1 div 0) AS result, typeof(coalesce(i, 1 div 0)) AS result_type
        FROM VALUES (0, 5), (1, CAST(NULL AS INT)) AS t(id, i)
        ORDER BY id
        """
      Then query result ordered
        | id | result | result_type |
        | 0  | 5      | bigint      |
        | 1  | NULL   | bigint      |
