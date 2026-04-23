@div
Feature: div (integer division) comprehensive tests

  # NOTE: Division-by-zero edge cases (literal and dynamic, both ANSI modes)
  # live in `divide_by_zero.feature`. This file focuses on type coverage,
  # overflow, sign/truncation, NULL propagation, intervals, and multi-row.

  Rule: Argument count validation

    Scenario: div zero args errors
      When query
        """
        SELECT div() AS result
        """
      Then query error .*

    Scenario: div one arg errors
      When query
        """
        SELECT div(10) AS result
        """
      Then query error .*

    Scenario: div three args errors
      When query
        """
        SELECT div(10, 3, 1) AS result
        """
      Then query error .*

  Rule: Basic integer division returns BIGINT

    Scenario: div positive integers
      When query
        """
        SELECT div(10, 3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div negative dividend truncates toward zero
      When query
        """
        SELECT div(-10, 3) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: div negative divisor truncates toward zero
      When query
        """
        SELECT div(10, -3) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: div both negative
      When query
        """
        SELECT div(-10, -3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div zero dividend
      When query
        """
        SELECT div(0, 5) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: div exact division
      When query
        """
        SELECT div(9, 3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div result is BIGINT regardless of input
      When query
        """
        SELECT div(CAST(6 AS TINYINT), CAST(2 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: NULL propagation on non-zero divisor

    Scenario: div NULL dividend returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INT), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div NULL divisor returns NULL
      When query
        """
        SELECT div(10, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div both NULL returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div untyped NULL dividend
      When query
        """
        SELECT div(NULL, 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Overflow semantics — LONG_MIN div -1

    @sail-bug
    # JVM wraps under ANSI=false (LONG_MIN / -1 = LONG_MIN in two's-complement);
    # Sail may propagate datafusion-style overflow handling. Mirrors the abs
    # TINYINT MIN wrap pattern (feedback_planner_hook_ship_gate).
    Scenario: div LONG_MIN by -1 wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |

    Scenario: div LONG_MIN by -1 errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query error .*

    Scenario: div INT MIN by -1 widens to BIGINT (no overflow)
      When query
        """
        SELECT div(-2147483648, -1) AS result
        """
      Then query result
        | result     |
        | 2147483648 |

  Rule: Mixed numeric types

    Scenario: div TINYINT by BIGINT widens to BIGINT
      When query
        """
        SELECT div(CAST(10 AS TINYINT), CAST(3 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div BIGINT by TINYINT widens to BIGINT
      When query
        """
        SELECT div(CAST(10 AS BIGINT), CAST(3 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div INT by DECIMAL
      When query
        """
        SELECT div(10, CAST(3.0 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div DECIMAL by INT truncates toward zero
      When query
        """
        SELECT div(CAST(3.0 AS DECIMAL(5,2)), 10) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: div DECIMAL by DECIMAL with different precision
      When query
        """
        SELECT div(CAST(10 AS DECIMAL(10,0)), CAST(3 AS DECIMAL(20,0))) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: DECIMAL division

    Scenario: div DECIMAL inputs returns BIGINT
      When query
        """
        SELECT div(CAST(7.5 AS DECIMAL(5,2)), CAST(2.5 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div DECIMAL truncates toward zero
      When query
        """
        SELECT div(CAST(10.9 AS DECIMAL(5,2)), CAST(3.0 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div negative DECIMAL truncates toward zero
      When query
        """
        SELECT div(CAST(-10.9 AS DECIMAL(5,2)), CAST(3.0 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | -3     |

  Rule: INTERVAL division

    Scenario: div two INTERVAL DAY
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '2' DAY) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div two INTERVAL YEAR
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '2' YEAR) AS result
        """
      Then query result
        | result |
        | 5      |

    @sail-bug
    # Known: Sail errors on interval div-by-zero even under ANSI=false.
    # Fix path: honour plan_config.ansi_mode in interval division kernel
    # (similar to how scalar div-by-zero is handled in divide_by_zero.feature).
    Scenario: div INTERVAL DAY by zero INTERVAL DAY returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '0' DAY) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div INTERVAL DAY by zero INTERVAL DAY errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '0' DAY) AS result
        """
      Then query error .*

    Scenario: div NULL INTERVAL returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INTERVAL DAY), INTERVAL '2' DAY) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Type rejection
    # Sail is more permissive than Spark with floating-point operands in div:
    # Spark rejects FLOAT and DOUBLE at analysis time (BINARY_OP_WRONG_TYPE /
    # BINARY_OP_DIFF_TYPES); Sail coerces and performs the division.
    # Fix path: type-validate div args in sail-plan's `spark_div` dispatcher
    # (reject FLOAT and DOUBLE at planning time to mirror Spark's analyzer).

    @sail-bug
    Scenario: div FLOAT/FLOAT errors
      When query
        """
        SELECT div(CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT)) AS result
        """
      Then query error .*

    @sail-bug
    Scenario: div DOUBLE/DOUBLE errors
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query error .*

    @sail-bug
    Scenario: div INT/DOUBLE errors
      When query
        """
        SELECT div(10, CAST(3.0 AS DOUBLE)) AS result
        """
      Then query error .*

  Rule: Workarounds for FLOAT and DOUBLE operands
    # Spark rejects FLOAT/DOUBLE in div by design (IntegralDivide requires
    # integral or decimal types). Users can still perform integer division
    # on floating-point values by casting first. These scenarios document
    # the valid workarounds.

    Scenario: div accepts DOUBLE values cast to DECIMAL
      When query
        """
        SELECT div(CAST(CAST(1.5 AS DOUBLE) AS DECIMAL(10,2)),
                   CAST(CAST(0.3 AS DOUBLE) AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div accepts DOUBLE values cast to BIGINT
      When query
        """
        SELECT div(CAST(CAST(10.7 AS DOUBLE) AS BIGINT),
                   CAST(CAST(3.2 AS DOUBLE) AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: regular division plus cast works with DOUBLE
      When query
        """
        SELECT CAST(CAST(1.5 AS DOUBLE) / CAST(0.3 AS DOUBLE) AS BIGINT) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div STRING/STRING errors
      When query
        """
        SELECT div('10', '3') AS result
        """
      Then query error .*

    Scenario: div BOOLEAN errors
      When query
        """
        SELECT div(true, true) AS result
        """
      Then query error .*

    Scenario: div DATE errors
      When query
        """
        SELECT div(DATE '2024-01-15', DATE '2024-01-01') AS result
        """
      Then query error .*

    Scenario: div INTERVAL DAY by INT errors
      When query
        """
        SELECT div(INTERVAL '10' DAY, 2) AS result
        """
      Then query error .*

    Scenario: div INTERVAL YEAR by INTERVAL DAY errors
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '2' DAY) AS result
        """
      Then query error .*

  Rule: Multi-row vectorized path

    Scenario: div BIGINT column with mixed signs and NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(10 AS BIGINT), CAST(0 AS BIGINT)),
          (CAST(NULL AS BIGINT), CAST(5 AS BIGINT)),
          (CAST(5 AS BIGINT), CAST(NULL AS BIGINT))
        AS t(a, b)
        """
      Then query result
        | result |
        | 3      |
        | -3     |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: div INT column exact division
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES (15, 3), (20, 5), (7, 2), (100, 10) AS t(a, b)
        """
      Then query result
        | result |
        | 5      |
        | 4      |
        | 3      |
        | 10     |
