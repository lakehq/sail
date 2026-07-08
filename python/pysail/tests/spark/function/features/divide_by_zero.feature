@divide_by_zero
Feature: Division by zero behavior

  Rule: All division by zero returns NULL when ANSI mode is disabled (Spark 4.x behavior)
    Scenario: Float divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Negative float divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -1.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Zero divided by zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 0.0 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Integer divided by integer zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Integer divided by float zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / 0.0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Decimal divided by decimal zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Decimal divided by integer zero returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(100.50 AS DECIMAL(10,2)) / 0 AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Division by zero throws error when ANSI mode is enabled
    Scenario: Integer divided by zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / 0 AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Float divided by zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1.0 / 0.0 AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Decimal divided by decimal zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Decimal divided by integer zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(100.50 AS DECIMAL(10,2)) / 0 AS result
        """
      Then query error (?i)divide.*zero

    Scenario: DIV by literal zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 DIV 0 AS result
        """
      Then query error (?i)divide.*zero

    Scenario: Modulo by literal zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query error (?i)remainder.*zero

    Scenario: Computed expression evaluating to zero throws error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / (1 - 1) AS result
        """
      Then query error (?i)division by zero

  Rule: Dynamic divisor division by zero raises error in ANSI mode
    Scenario: Integer divided by dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 / id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)division by zero

    Scenario: Decimal divided by dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) / CAST(id AS DECIMAL(10,2)) AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)division by zero

    Scenario: Double divided by dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(1 AS DOUBLE) / CAST(id AS DOUBLE) AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)division by zero

    Scenario: DIV by dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 DIV id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)division by zero

    Scenario: Modulo by dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 % id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)remainder.*zero

    Scenario: mod function with dynamic zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT mod(10, id) AS result FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)remainder.*zero

    Scenario: Division by zero in range raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / id AS result FROM range(2)
        """
      Then query error (?i)division by zero

    Scenario: Two-column division with zero divisor raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a, b, a / b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query error (?i)division by zero

    Scenario: Two-column modulo with zero divisor raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a, b, a % b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query error (?i)remainder.*zero

  Rule: Dynamic divisor division by zero returns NULL in non-ANSI mode
    Scenario: Integer divided by dynamic zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 / id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

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

    Scenario: DIV by dynamic zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 DIV id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Modulo by dynamic zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 % id AS result FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

  Rule: DIV and modulo by literal zero returns NULL in non-ANSI mode
    Scenario: DIV by literal zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 DIV 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Modulo by literal zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 10 % 0 AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Computed expression evaluating to zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / (1 - 1) AS result
        """
      Then query result
        | result |
        | NULL   |

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
    Scenario: year-month interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_ym_interval(1, 6) / 0 AS result
        """
      Then query error (?i)division by zero

    Scenario: year-month interval divided by zero also raises under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_ym_interval(1, 6) / 0 AS result
        """
      Then query error (?i)division by zero

    # make_interval produces Spark's legacy CalendarInterval, whose divide throws
    # only under ANSI and returns NULL otherwise.
    Scenario: calendar interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 1, 0, 0, 0) / 0 AS result
        """
      Then query error (?i)division by zero

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
    Scenario: day-time interval divided by zero raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT INTERVAL '1' DAY / 0 AS result
        """
      Then query error (?i)division by zero

    Scenario: day-time interval divided by zero also raises under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT INTERVAL '1' DAY / 0 AS result
        """
      Then query error (?i)division by zero
