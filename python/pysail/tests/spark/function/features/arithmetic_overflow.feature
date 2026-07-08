@arithmetic_overflow
Feature: ANSI overflow semantics for the +, -, * operators

  # Under spark.sql.ansi.enabled=true an integral overflow raises
  # ARITHMETIC_OVERFLOW; under ANSI off it wraps two's complement (integrals) or
  # returns NULL (decimals). Paired ANSI on/off per scenario.

  Rule: Integer addition overflow
    Scenario: integer add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer add overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

  Rule: Integer subtraction overflow
    Scenario: integer subtract overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer subtract overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS result
        """
      Then query result
        | result     |
        | 2147483647 |

  Rule: Integer multiplication overflow
    Scenario: integer multiply overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer multiply overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: Narrow integer overflow raises under ANSI on, wraps (keeping the type) under ANSI off
    Scenario: tinyint add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT) AS result
        """
      Then query error (?i)overflow

    Scenario: tinyint add overflow wraps and stays tinyint under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT) AS result
        """
      Then query result
        | result |
        | -128   |
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: tinyint subtract overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(-128 AS TINYINT) - CAST(1 AS TINYINT) AS result
        """
      Then query error (?i)overflow

    Scenario: tinyint subtract overflow wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(-128 AS TINYINT) - CAST(1 AS TINYINT) AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: smallint multiply overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(32767 AS SMALLINT) * CAST(2 AS SMALLINT) AS result
        """
      Then query error (?i)overflow

    Scenario: smallint multiply overflow wraps and stays smallint under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(32767 AS SMALLINT) * CAST(2 AS SMALLINT) AS result
        """
      Then query result
        | result |
        | -2     |
      Then query schema
        """
        root
         |-- result: short (nullable = false)
        """

    Scenario: bigint add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS result
        """
      Then query error (?i)overflow

    Scenario: bigint add overflow wraps to LONG_MIN under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

  Rule: Non-finite doubles pass through in every mode (Inf/NaN are not overflow)
    # Float Inf/NaN is a valid IEEE 754 result, not an overflow, so `+ - *` on
    # doubles never raises/NULLs — identical under ANSI on and off.
    Scenario: infinity plus infinity is infinity (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) + CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: infinity plus infinity is infinity (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) + CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: infinity minus infinity is NaN (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) - CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity minus infinity is NaN (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) - CAST('Infinity' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity times zero is NaN (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity times zero is NaN (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: NaN propagates through addition
      When query
        """
        SELECT CAST('NaN' AS DOUBLE) + CAST(2.0 AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: Decimal overflow raises under ANSI on, NULL under ANSI off
    Scenario: decimal add overflow raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS result
        """
      Then query error (?i)overflow

    Scenario: decimal add overflow returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS result
        """
      Then query result
        | result |
        | NULL   |

  # ANSI `+`/`-`/`*` normally route through the SparkAdd/Subtract/Multiply UDFs.
  # When an operand contains a scalar subquery the builder falls back to the
  # native operator (see `has_scalar_subquery` in math.rs), because wrapping a
  # subquery in the UDF breaks the physical planner's count-bug check inside a
  # correlated subquery body. The two scenarios below demonstrate both sides:
  # the guarded case keeps a native `+`, plain arithmetic keeps `spark_add`.
  # @sail-only: these assert the physical plan, which differs under distributed
  # (server) execution, so they run only against the in-process engine.
  @sail-only
  Rule: Arithmetic with a scalar subquery operand (subquery guard)
    # Breaking case (guarded): a correlated outer subquery whose body adds an
    # UNCORRELATED nested subquery. Without the guard this fails to plan with
    # "does not support logical expression ScalarSubquery" under ANSI on. With
    # the guard the arithmetic stays a native `+` (see the plan) and computes.
    Scenario: correlated subquery adding a nested subquery stays native under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT outer_t.k, (
          SELECT max(inner_t.x) + (SELECT min(y) FROM VALUES (1) AS u(y))
          FROM VALUES (1, 4), (2, 3) AS inner_t(k, x)
          WHERE inner_t.k = outer_t.k
        ) AS s
        FROM VALUES (1, 2), (2, 3) AS outer_t(k, v)
        ORDER BY outer_t.k
        """
      Then query result ordered
        | k | s |
        | 1 | 5 |
        | 2 | 4 |
      When query
        """
        EXPLAIN SELECT outer_t.k, (
          SELECT max(inner_t.x) + (SELECT min(y) FROM VALUES (1) AS u(y))
          FROM VALUES (1, 4), (2, 3) AS inner_t(k, x)
          WHERE inner_t.k = outer_t.k
        ) AS s
        FROM VALUES (1, 2), (2, 3) AS outer_t(k, v)
        ORDER BY outer_t.k
        """
      Then query plan matches snapshot

    # Normal case (contrast): plain ANSI integer `+` with no subquery operand —
    # the guard does not fire, so the plan keeps the `spark_add` UDF that carries
    # the ANSI overflow check.
    Scenario: plain ANSI integer add keeps the spark_add UDF under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT v + 1 AS result FROM VALUES (1), (2) AS t(v) ORDER BY v
        """
      Then query result ordered
        | result |
        | 2      |
        | 3      |
      When query
        """
        EXPLAIN SELECT v + 1 AS result FROM VALUES (1), (2) AS t(v) ORDER BY v
        """
      Then query plan matches snapshot

  # The subquery guard drops the arithmetic to the native operator, which wraps
  # on overflow instead of raising. Spark still raises ARITHMETIC_OVERFLOW even
  # when an operand is a scalar subquery (the subquery only defers the check to
  # runtime). This is the guard's one cost — a real divergence under ANSI on.
  # The root fix lives in the DataFusion fork (`evaluates_to_null`); once it
  # lands the guard can be dropped and this @sail-bug removed.
  Rule: Overflow with a scalar subquery operand is undetected under ANSI (guard cost)
    @sail-bug
    Scenario: integer overflow with a subquery operand raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT (SELECT max(v) FROM VALUES (CAST(2147483647 AS INT)) AS u(v)) + CAST(1 AS INT) AS result
        """
      Then query error (?i)overflow

    Scenario: integer overflow with a subquery operand wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT (SELECT max(v) FROM VALUES (CAST(2147483647 AS INT)) AS u(v)) + CAST(1 AS INT) AS result
        """
      Then query result
        | result      |
        | -2147483648 |
