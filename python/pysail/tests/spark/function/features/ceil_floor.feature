@ceil @floor
Feature: ceil() and floor() round numbers toward +/- infinity

  Rule: ceil basic

    Scenario: positive integer
      When query
        """
        SELECT ceil(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: zero
      When query
        """
        SELECT ceil(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: positive double rounds up
      When query
        """
        SELECT ceil(1.1) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: negative double rounds toward zero
      When query
        """
        SELECT ceil(-1.9) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: negative small value
      When query
        """
        SELECT ceil(-0.1) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: float input
      When query
        """
        SELECT ceil(CAST(1.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: decimal input
      When query
        """
        SELECT ceil(CAST(1.5 AS DECIMAL(2,1))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceiling alias
      When query
        """
        SELECT ceiling(1.5) AS result
        """
      Then query result
        | result |
        | 2      |

  Rule: floor basic

    Scenario: positive integer
      When query
        """
        SELECT floor(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: zero
      When query
        """
        SELECT floor(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: positive double rounds down
      When query
        """
        SELECT floor(1.9) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: negative double rounds away from zero
      When query
        """
        SELECT floor(-1.1) AS result
        """
      Then query result
        | result |
        | -2     |

    Scenario: negative small value
      When query
        """
        SELECT floor(-0.1) AS result
        """
      Then query result
        | result |
        | -1     |

  Rule: NULL handling (1-arg)

    Scenario: untyped NULL ceil
      When query
        """
        SELECT ceil(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: untyped NULL floor
      When query
        """
        SELECT floor(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double
      When query
        """
        SELECT ceil(CAST(NULL AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL integer
      When query
        """
        SELECT ceil(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL decimal
      When query
        """
        SELECT ceil(CAST(NULL AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL handling (2-arg)

    Scenario: untyped NULL with positive scale
      When query
        """
        SELECT ceil(NULL, 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: untyped NULL with negative scale
      When query
        """
        SELECT ceil(NULL, -1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double with positive scale
      When query
        """
        SELECT ceil(CAST(NULL AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL double with negative scale
      When query
        """
        SELECT floor(CAST(NULL AS DOUBLE), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Two-arg with scale equal to input scale (no change)

    Scenario: ceil(1.5, 1)
      When query
        """
        SELECT ceil(1.5, 1) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: floor(1.5, 1)
      When query
        """
        SELECT floor(1.5, 1) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: ceil(1.23, 2)
      When query
        """
        SELECT ceil(1.23, 2) AS result
        """
      Then query result
        | result |
        | 1.23   |

  Rule: Two-arg with scale greater than input (value unchanged)

    Scenario: scale 2 on decimal(2,1) — ceil
      When query
        """
        SELECT ceil(1.5, 2) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 2 on decimal(2,1) — floor
      When query
        """
        SELECT floor(1.5, 2) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 10 on decimal(2,1)
      When query
        """
        SELECT ceil(1.5, 10) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 38 is still valid
      When query
        """
        SELECT ceil(1.5, 38) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: scale 100 beyond decimal128 max, value unchanged
      When query
        """
        SELECT ceil(1.5, 100) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: zero decimal with large scale
      When query
        """
        SELECT ceil(CAST(0 AS DECIMAL(5,2)), 5) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Two-arg with scale less than input (rounds)

    Scenario: ceil(1.234, 2) rounds up
      When query
        """
        SELECT ceil(1.234, 2) AS result
        """
      Then query result
        | result |
        | 1.24   |

    Scenario: floor(1.234, 2) truncates
      When query
        """
        SELECT floor(1.234, 2) AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: ceil(1.234, 0)
      When query
        """
        SELECT ceil(1.234, 0) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: negative value ceil moves toward zero
      When query
        """
        SELECT ceil(-1.25, 1) AS result
        """
      Then query result
        | result |
        | -1.2   |

    Scenario: negative value floor moves away from zero
      When query
        """
        SELECT floor(-1.25, 1) AS result
        """
      Then query result
        | result |
        | -1.3   |

  Rule: Two-arg with negative scale (rounds left of decimal)

    Scenario: ceil(123.456, -1)
      When query
        """
        SELECT ceil(123.456, -1) AS result
        """
      Then query result
        | result |
        | 130    |

    Scenario: floor(123.456, -1)
      When query
        """
        SELECT floor(123.456, -1) AS result
        """
      Then query result
        | result |
        | 120    |

    Scenario: ceil(123.456, -2)
      When query
        """
        SELECT ceil(123.456, -2) AS result
        """
      Then query result
        | result |
        | 200    |

    Scenario: ceil(999.99, -1) crosses boundary
      When query
        """
        SELECT ceil(999.99, -1) AS result
        """
      Then query result
        | result |
        | 1000   |

    Scenario: scale -37 is the max negative scale that fits Decimal128
      When query
        """
        SELECT ceil(123.456, -37) AS result
        """
      Then query result
        | result                                  |
        | 10000000000000000000000000000000000000  |

    Scenario: ceil negative with negative scale
      When query
        """
        SELECT ceil(-999.99, -1) AS result
        """
      Then query result
        | result |
        | -990   |

    Scenario: floor negative with negative scale
      When query
        """
        SELECT floor(-999.99, -1) AS result
        """
      Then query result
        | result |
        | -1000  |

  Rule: Integer input with scale

    Scenario: int with zero scale
      When query
        """
        SELECT ceil(CAST(5 AS INT), 0) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: int with positive scale (no effect)
      When query
        """
        SELECT ceil(CAST(5 AS INT), 2) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: int with negative scale
      When query
        """
        SELECT ceil(CAST(5 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: floor int with negative scale
      When query
        """
        SELECT floor(CAST(5 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: tinyint with negative scale
      When query
        """
        SELECT ceil(CAST(125 AS TINYINT), -1) AS result
        """
      Then query result
        | result |
        | 130    |

    Scenario: bigint zero with negative scale
      When query
        """
        SELECT ceil(CAST(0 AS BIGINT), -5) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Float and Double with scale

    Scenario: float with scale includes trailing zero
      When query
        """
        SELECT ceil(CAST(1.5 AS FLOAT), 2) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: double with scale includes trailing zero
      When query
        """
        SELECT ceil(CAST(1.5 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: very small double rounds to zero
      When query
        """
        SELECT ceil(CAST(1e-300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Special float values (1-arg) — NaN/Infinity clamp to integer bounds

    Scenario: Infinity to LONG_MAX
      When query
        """
        SELECT ceil(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: -Infinity to LONG_MIN
      When query
        """
        SELECT ceil(CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

    Scenario: NaN to zero
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: floor Infinity
      When query
        """
        SELECT floor(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

  Rule: Special float values with scale (2-arg) — Spark returns NULL

    Scenario: NaN with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NaN with negative scale returns NULL
      When query
        """
        SELECT ceil(CAST('NaN' AS DOUBLE), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('Infinity' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: -Infinity with positive scale returns NULL
      When query
        """
        SELECT ceil(CAST('-Infinity' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: floor NaN with scale returns NULL
      When query
        """
        SELECT floor(CAST('NaN' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Negative zero

    Scenario: ceil -0.0 returns 0
      When query
        """
        SELECT ceil(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: floor -0.0 returns 0
      When query
        """
        SELECT floor(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: ceil -0.0 with scale returns 0.00
      When query
        """
        SELECT ceil(CAST(-0.0 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | 0.00   |

  Rule: Multi-row propagation

    Scenario: mix of values, NaN, Inf, NULL — 1-arg
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW mx AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(-1.5 AS DOUBLE)),
          (CAST(0.0 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v, ceil(v) AS c, floor(v) AS f FROM mx ORDER BY v NULLS LAST
        """
      Then query result ordered
        | v    | c    | f    |
        | -1.5 | -1   | -2   |
        | 0.0  | 0    | 0    |
        | 1.5  | 2    | 1    |
        | NULL | NULL | NULL |

    Scenario: all-NULL column
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW n1 AS SELECT CAST(NULL AS DOUBLE) AS v
        """
      When query
        """
        SELECT ceil(v) AS c, floor(v) AS f FROM n1
        """
      Then query result
        | c    | f    |
        | NULL | NULL |

    Scenario: empty DataFrame returns empty
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW e1 AS SELECT CAST(1.0 AS DOUBLE) AS v WHERE false
        """
      When query
        """
        SELECT ceil(v) AS result FROM e1
        """
      Then query result
        | result |

  Rule: Algebraic simplification (idempotent)

    Scenario: ceil of ceil is ceil
      When query
        """
        SELECT ceil(ceil(1.9)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: floor of floor is floor
      When query
        """
        SELECT floor(floor(1.9)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: triple nested ceil collapses
      When query
        """
        SELECT ceil(ceil(ceil(1.9))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil of floor cascades (floor returns integer, ceil identity)
      When query
        """
        SELECT ceil(floor(1.9)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: floor of ceil cascades
      When query
        """
        SELECT floor(ceil(1.1)) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: ceil of integer is integer (no rounding)
      When query
        """
        SELECT ceil(CAST(7 AS INT)) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: floor of integer is integer (no rounding)
      When query
        """
        SELECT floor(CAST(-42 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | -42    |

  Rule: Filter pushdown — WHERE ceil/floor(col) OP constant

    Scenario: WHERE ceil(col) > N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) > 2 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 2.1 |
        | 5.5 |

    Scenario: WHERE floor(col) <= N keeps correct rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) <= 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 0.5 |
        | 1.1 |
        | 1.9 |

    Scenario: WHERE ceil(col) BETWEEN
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(3.0 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) BETWEEN 2 AND 3 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 1.1 |
        | 1.9 |
        | 2.1 |
        | 3.0 |

    Scenario: WHERE ceil on integer column (identity after simplify)
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (1), (5), (10), (CAST(NULL AS INT)) AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE ceil(v) > 3 ORDER BY v
        """
      Then query result ordered
        | v  |
        | 5  |
        | 10 |

    Scenario: WHERE floor(col) returns NULL excludes NULL rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST(2.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT count(*) AS c FROM vals WHERE floor(v) IS NOT NULL
        """
      Then query result
        | c |
        | 2 |

    # Exercises the preimage rewrite: `floor(v) = N` becomes
    # `v >= N AND v < N + 1`. Result must still match Spark row-for-row.
    Scenario: WHERE floor(col) = N keeps the right rows
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.0 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE)),
          (CAST(-0.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT v FROM vals WHERE floor(v) = 1 ORDER BY v
        """
      Then query result ordered
        | v   |
        | 1.0 |
        | 1.9 |

    Scenario: GROUP BY ceil(v) aggregates correctly
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW grp_vals AS SELECT * FROM VALUES
          (CAST(0.5 AS DOUBLE)),
          (CAST(1.1 AS DOUBLE)),
          (CAST(1.9 AS DOUBLE)),
          (CAST(2.1 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)),
          (CAST(5.5 AS DOUBLE))
        AS t(v)
        """
      When query
        """
        SELECT ceil(v) AS bucket, count(*) AS n
        FROM grp_vals GROUP BY ceil(v) ORDER BY bucket
        """
      Then query result ordered
        | bucket | n |
        | 1      | 1 |
        | 2      | 2 |
        | 3      | 2 |
        | 6      | 1 |

  Rule: Error conditions

    Scenario: non-foldable scale errors
      When query
        """
        SELECT ceil(1.5, CAST(NULL AS INT)) AS result
        """
      Then query error .*

    Scenario: too-negative scale errors
      When query
        """
        SELECT ceil(1.5, -100) AS result
        """
      Then query error .*

    Scenario: non-INT scale type errors
      When query
        """
        SELECT ceil(1.5, CAST(2 AS BIGINT)) AS result
        """
      Then query error .*

    Scenario: scale -38 overflows decimal128 precision
      When query
        """
        SELECT ceil(123.456, -38) AS result
        """
      Then query error .*

    Scenario: ceil very large double with scale overflows decimal
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*

  Rule: Scale -37 boundary (max negative scale that fits Decimal128)

    Scenario: ceil scale -37 returns 10^37
      When query
        """
        SELECT ceil(1.5, -37) AS result
        """
      Then query result
        | result                                  |
        | 10000000000000000000000000000000000000  |

    Scenario: floor scale -37 truncates small value to zero
      When query
        """
        SELECT floor(1.5, -37) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: ANSI mode on overflow

    # Fixed 2026-04-21: SparkCeil/SparkFloor now carry ansi_mode: bool state bound
    # at planning time from PlanConfig::ansi_mode (serialized via protobuf
    # SparkCeilUdf/SparkFloorUdf for distributed execution). Under ANSI=false,
    # overflow in the Float→Decimal cast becomes NULL; under ANSI=true it errors.
    # Both UDFs share the spark_ceil_floor() helper — testing all 4 combinations
    # (ceil/floor × ANSI=true/false) guards against regressions in either wrapper.

    Scenario: ceil ANSI=false overflow returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Spark JVM raises [NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION] when the
    # Float→Decimal cast under ANSI=true overflows. Sail's cast kernel emits a
    # raw arrow error without a bracketed Spark error class. The query DOES
    # error in both engines (loose `.*` would silently pass), but the class
    # diverges. Fix path: align Sail's Float→Decimal cast error to use Spark's
    # error class, likely in arrow-rs cast kernel or a Sail-side wrapper.
    Scenario: ceil ANSI=true overflow errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ceil(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*\[NUMERIC_VALUE_OUT_OF_RANGE.*\].*

    Scenario: floor ANSI=false overflow returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT floor(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Same root cause as `ceil ANSI=true overflow` — Sail's cast kernel emits
    # a raw arrow error instead of [NUMERIC_VALUE_OUT_OF_RANGE].
    Scenario: floor ANSI=true overflow errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT floor(CAST(1e300 AS DOUBLE), 2) AS result
        """
      Then query error .*\[NUMERIC_VALUE_OUT_OF_RANGE.*\].*

  Rule: Adversarial — deep nesting stress (simplify chain survival)
    # 5-level mixed nesting. Spark JVM collapses to the outermost function's
    # semantic. Our simplify handles same-fn collapse + integer-identity, but
    # does NOT fold `ceil(floor(x))` pairs directly (cross-nesting is handled
    # by the integer-identity + cast-folding cascade: floor(x) returns Int64,
    # then ceil(Int64) simplifies to `cast(floor(x), Int64)` which folds out).
    # Row-result confirms correctness end-to-end.

    Scenario: 5-level mixed ceil/floor collapses semantically
      When query
        """
        SELECT ceil(floor(ceil(floor(ceil(CAST(1.5 AS DOUBLE)))))) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: 5-level alternating with negative input
      When query
        """
        SELECT floor(ceil(floor(ceil(floor(CAST(-1.5 AS DOUBLE)))))) AS result
        """
      Then query result
        | result |
        | -2     |


  Rule: Adversarial — identity-on-subtype at type boundaries
    # ceil(BIGINT) and floor(BIGINT) are identity by simplify rewrite.
    # Exercise at BIGINT boundary to confirm no overflow in the identity path.

    Scenario: ceil BIGINT_MAX is identity
      When query
        """
        SELECT ceil(CAST(9223372036854775807 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: floor BIGINT_MIN is identity
      When query
        """
        SELECT floor(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |


  Rule: Adversarial — preimage edge cases (floor-only filter pushdown)
    # `preimage` on SparkFloor must handle: non-integer RHS (return None),
    # BIGINT boundary RHS (watch for overflow in N+1), NULL RHS.

    Scenario: floor with non-integer RHS — filter matches nothing
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 2.5
        """
      Then query result
        | v |

    Scenario: floor equals BIGINT_MAX boundary — no rows match in sample
      When query
        """
        SELECT v FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)) AS t(v)
        WHERE floor(v) = 9223372036854775807
        """
      Then query result
        | v |
