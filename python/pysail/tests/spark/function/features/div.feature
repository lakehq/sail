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
    # Two's-complement: BIGINT range is [-2^63, 2^63-1]; -LONG_MIN overflows
    # BIGINT, so under ANSI=false Spark wraps to LONG_MIN (matches Java
    # Math.floorDiv); ANSI=true raises ARITHMETIC_OVERFLOW.

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

    Scenario: div LONG_MAX by 1 is identity
      When query
        """
        SELECT div(CAST(9223372036854775807 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: div LONG_MAX by -1 negates without overflow
      When query
        """
        SELECT div(CAST(9223372036854775807 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result               |
        | -9223372036854775807 |

    Scenario: div zero by zero returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(0 AS BIGINT), CAST(0 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div zero by zero errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(0 AS BIGINT), CAST(0 AS BIGINT)) AS result
        """
      Then query error .*

    Scenario: div BIGINT column containing LONG_MIN with -1 wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)),
          (CAST(-10 AS BIGINT), CAST(2 AS BIGINT))
        AS t(a, b)
        """
      Then query result
        | result                |
        | 3                     |
        | -9223372036854775808  |
        | -5                    |

    Scenario: div BIGINT column containing LONG_MIN with -1 errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT))
        AS t(a, b)
        """
      Then query error .*

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

    Scenario: div INTERVAL DAY multi-row with zero divisor returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result FROM VALUES
          (INTERVAL '10' DAY, INTERVAL '2' DAY),
          (INTERVAL '5' DAY, INTERVAL '0' DAY)
        AS t(a, b)
        """
      Then query result
        | result |
        | 5      |
        | NULL   |

    Scenario: div INTERVAL YEAR by zero INTERVAL YEAR returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '0' YEAR) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div INTERVAL YEAR by zero INTERVAL YEAR errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '0' YEAR) AS result
        """
      Then query error .*

    Scenario: div INTERVAL DAY multi-row with zero divisor errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES
          (INTERVAL '10' DAY, INTERVAL '2' DAY),
          (INTERVAL '5' DAY, INTERVAL '0' DAY)
        AS t(a, b)
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
    # Spark's IntegralDivide rejects FLOAT and DOUBLE at analysis time
    # (BINARY_OP_WRONG_TYPE / BINARY_OP_DIFF_TYPES). Sail mirrors that in
    # the `spark_div` dispatcher — see workarounds below for valid patterns.

    Scenario: div FLOAT/FLOAT errors
      When query
        """
        SELECT div(CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT)) AS result
        """
      Then query error .*

    Scenario: div DOUBLE/DOUBLE errors
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query error .*

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
