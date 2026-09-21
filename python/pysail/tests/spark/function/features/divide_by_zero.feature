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

  Rule: Per-row division by zero in a multi-row batch, both ANSI modes
    # A batch where only some rows have a zero divisor: ANSI off nulls exactly the
    # offending rows (per element) and computes the rest; ANSI on raises for the
    # whole batch.
    Scenario: mixed integer divisors null only the zero rows under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a / b AS result FROM VALUES (6, 2), (1, 0), (8, 4) AS t(a, b)
        """
      Then query result ordered
        | result |
        | 3.0    |
        | NULL   |
        | 2.0    |

    Scenario: mixed integer divisors raise for the whole batch under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a / b AS result FROM VALUES (6, 2), (1, 0), (8, 4) AS t(a, b)
        """
      Then query error (?i)division by zero

    @sail-bug
    Scenario: mixed decimal divisors null only the zero rows under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(a AS DECIMAL(10,2)) / CAST(b AS DECIMAL(10,2)) AS result
        FROM VALUES (6, 2), (1, 0), (8, 4) AS t(a, b)
        """
      Then query result ordered
        | result          |
        | 3.0000000000000 |
        | NULL            |
        | 2.0000000000000 |

    Scenario: mixed decimal divisors raise for the whole batch under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(a AS DECIMAL(10,2)) / CAST(b AS DECIMAL(10,2)) AS result
        FROM VALUES (6, 2), (1, 0), (8, 4) AS t(a, b)
        """
      Then query error (?i)division by zero

  Rule: Interval division by zero
    # Year-month intervals (Spark's DivideYMInterval) throw INTERVAL_DIVIDED_BY_ZERO
    # on a zero divisor in BOTH ANSI modes; only try_divide tolerates it as NULL.
    @sail-bug
    Scenario: year-month interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_ym_interval(1, 6) / 0 AS result
        """
      Then query error (?i)division by zero

    @sail-bug
    Scenario: year-month interval divided by zero also raises under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_ym_interval(1, 6) / 0 AS result
        """
      Then query error (?i)division by zero

    # make_interval produces Spark's legacy CalendarInterval, whose divide throws
    # only under ANSI and returns NULL otherwise. @spark-4: the PySpark 3.5 client
    # sends this as the `calendar_interval` type, which Sail rejects at analysis
    # ("Unsupported data type calendar_interval") before the divide runs; 4.x
    # clients send `month_day_nano_interval` and reach the divide-by-zero path.
    @sail-bug
    @spark-4
    Scenario: calendar interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 1, 0, 0, 0) / 0 AS result
        """
      Then query error (?i)division by zero

    @spark-4
    Scenario: calendar interval divided by zero returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(0, 0, 0, 1, 0, 0, 0) / 0 AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: try_divide tolerates interval division by zero as NULL in both ANSI modes
    Scenario: try_divide year-month interval by zero is NULL under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(make_ym_interval(1, 6), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_divide year-month interval by zero is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(make_ym_interval(1, 6), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Day-time interval division by zero raises in both ANSI modes
    # Spark's DivideDTInterval throws INTERVAL_DIVIDED_BY_ZERO unconditionally,
    # unlike numeric division which nulls under ANSI off.
    @sail-bug
    Scenario: day-time interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT INTERVAL '1' DAY / 0 AS result
        """
      Then query error (?i)division by zero

    @sail-bug
    Scenario: day-time interval divided by zero also raises under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT INTERVAL '1' DAY / 0 AS result
        """
      Then query error (?i)division by zero
