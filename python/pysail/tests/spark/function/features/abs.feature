@abs
Feature: abs comprehensive tests

  Rule: Argument count validation

    Scenario: abs zero args errors
      When query
        """
        SELECT abs() AS result
        """
      Then query error .*

    Scenario: abs two args errors
      When query
        """
        SELECT abs(1, 2) AS result
        """
      Then query error .*

  Rule: NULL propagation

    Scenario: abs untyped NULL
      When query
        """
        SELECT abs(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed INT
      When query
        """
        SELECT abs(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed TINYINT
      When query
        """
        SELECT abs(CAST(NULL AS TINYINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed SMALLINT
      When query
        """
        SELECT abs(CAST(NULL AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed BIGINT
      When query
        """
        SELECT abs(CAST(NULL AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed FLOAT
      When query
        """
        SELECT abs(CAST(NULL AS FLOAT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed DOUBLE
      When query
        """
        SELECT abs(CAST(NULL AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed DECIMAL
      When query
        """
        SELECT abs(CAST(NULL AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NULL typed INTERVAL DAY TO SECOND
      When query
        """
        SELECT abs(CAST(NULL AS INTERVAL DAY TO SECOND)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic integer types happy path

    Scenario: abs negative INT
      When query
        """
        SELECT abs(-5) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: abs positive INT
      When query
        """
        SELECT abs(5) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: abs zero INT
      When query
        """
        SELECT abs(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: abs negative TINYINT safe
      When query
        """
        SELECT abs(CAST(-127 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 127    |

    Scenario: abs negative SMALLINT safe
      When query
        """
        SELECT abs(CAST(-32767 AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | 32767  |

    Scenario: abs negative BIGINT safe
      When query
        """
        SELECT abs(CAST(-9223372036854775807 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

  Rule: Float and double values

    Scenario: abs negative DOUBLE
      When query
        """
        SELECT abs(CAST(-1.5 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: abs positive FLOAT
      When query
        """
        SELECT abs(CAST(1.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: abs DOUBLE negative zero
      When query
        """
        SELECT abs(CAST(-0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: abs FLOAT negative zero
      When query
        """
        SELECT abs(CAST(-0.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: abs DOUBLE NaN
      When query
        """
        SELECT abs(CAST('NaN' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: abs DOUBLE Infinity
      When query
        """
        SELECT abs(CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: abs DOUBLE negative Infinity
      When query
        """
        SELECT abs(CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: Decimal values

    Scenario: abs negative DECIMAL
      When query
        """
        SELECT abs(CAST(-1.5 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: abs DECIMAL zero
      When query
        """
        SELECT abs(CAST(0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 0.00   |

    Scenario: abs DECIMAL very small
      When query
        """
        SELECT abs(CAST(-0.001 AS DECIMAL(10,3))) AS result
        """
      Then query result
        | result |
        | 0.001  |

    @sail-bug
    # Tagged @sail-bug purely for Spark-compat tracking — Sail's behaviour here
    # is arguably MORE correct mathematically. Divergence lives in CAST, not abs:
    # JVM applies half-up rounding during CAST to DECIMAL(38,0) and rounds 37
    # nines up to 10^37; Sail preserves precision and returns 37 nines. Whether
    # to "fix" this (align with Spark) or keep Sail's precise behaviour is a
    # policy call. Out of scope for `abs` either way — fix path is the decimal
    # CAST kernel (arrow-rs `cast_decimal` semantics or a Sail-side override).
    Scenario: abs DECIMAL 38,0 near max
      When query
        """
        SELECT abs(CAST(-9999999999999999999999999999999999999 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result                                  |
        | 10000000000000000000000000000000000000  |

    @sail-bug
    # Same root cause as the scenario above (CAST rounding) — JVM rounds
    # 38 nines up to 10^38 and errors on overflow; Sail keeps 38 nines.
    Scenario: abs DECIMAL 38,0 exceeds range errors
      When query
        """
        SELECT abs(CAST(-99999999999999999999999999999999999999 AS DECIMAL(38,0))) AS result
        """
      Then query error .*

  Rule: Integer overflow under ANSI=false wraps to MIN
    # Two's-complement quirk: signed integer range is asymmetric (e.g. TINYINT
    # is [-128, 127]), so -MIN cannot be represented in the same width. Spark
    # under ANSI=false matches Java's Math.abs(int) and returns MIN itself
    # (wrap-around) instead of erroring. ANSI=true raises ARITHMETIC_OVERFLOW.

    Scenario: abs TINYINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-128 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | -128   |

    Scenario: abs SMALLINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-32768 AS SMALLINT)) AS result
        """
      Then query result
        | result |
        | -32768 |

    Scenario: abs INT MIN via CAST wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-2147483648 AS INT)) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    Scenario: abs BIGINT MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |

    @sail-bug
    # Sail promotes the literal to BIGINT; JVM keeps INT and wraps to MIN.
    # Root cause: Sail's SQL parses `-2147483648` as unary-minus + positive
    # literal; the positive side overflows INT32 (max 2147483647) and gets
    # widened to BIGINT. Spark has a special rule that recognises the whole
    # `-INT32_MIN` (and `-LONG_MIN`) literal and keeps the narrow type.
    # Fix path: `sail-sql-analyzer` (or parser) — add constant-folding rule
    # for `UnaryMinus(IntegerLiteral(N))` that narrows when `-N` fits in a
    # smaller signed type. Affects every expression with negative-MIN
    # literals, not just abs.
    Scenario: abs INT literal MIN preserves INT type and wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(-2147483648) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

  Rule: Integer overflow under ANSI=true errors

    Scenario: abs TINYINT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-128 AS TINYINT)) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

    Scenario: abs INT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-2147483648 AS INT)) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

    Scenario: abs BIGINT MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(CAST(-9223372036854775808 AS BIGINT)) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

  Rule: String coercion under ANSI=false
    # Sail now coerces STRING → DOUBLE (via `coerce_types` in spark_abs), but
    # the inserted CAST does not honour `spark.sql.ansi.enabled`. Under
    # ANSI=false, Spark returns NULL for unparseable strings (`'hello'`, `''`,
    # whitespace-only `'   '`); Sail errors in both modes. Whitespace-padded
    # numeric strings (`'  -5  '`) ARE parseable by Java's Double.parseDouble
    # and Spark accepts them — they are not in the "unparseable" set.
    # Fix path: make Sail's CAST ANSI-aware (propagate `plan_config.ansi_mode`
    # into `CastOptions { safe: !ansi }` when wrapping the coerced expr).
    # Affects every UDF that coerces STRING → numeric, not just abs.

    Scenario: abs negative numeric string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('-5') AS result
        """
      Then query result
        | result |
        | 5.0    |

    Scenario: abs numeric string with decimal
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('5.5') AS result
        """
      Then query result
        | result |
        | 5.5    |

    @sail-bug
    Scenario: abs whitespace-padded numeric string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('  -5  ') AS result
        """
      Then query result
        | result |
        | 5.0    |

    @sail-bug
    Scenario: abs non-numeric string returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('hello') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: abs empty string returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: abs NaN string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('NaN') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: abs Infinity string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs('Infinity') AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: Interval values
    # abs preserves the Arrow interval unit, but Sail widens Spark subranges
    # (DAY, HOUR TO MINUTE, ...) to DAY TO SECOND at the type layer — this
    # happens even without abs (e.g. SELECT INTERVAL '-5' DAY returns DAY TO
    # SECOND). The scenarios below are tagged @sail-bug but blocked on the
    # Sail-wide interval subrange handling, not on abs itself.
    # Fix path: preserve Spark subrange (DAY, HOUR, DAY TO SECOND, …) as
    # `Field` metadata when converting Spark→Arrow, restore on the return
    # trip in `sail-spark-connect`. Also requires analyzer changes in
    # `sail-sql-analyzer`. Affects every expression returning intervals.

    @sail-bug
    Scenario: abs negative INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '-5' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '5' DAY |

    @sail-bug
    Scenario: abs positive INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '5' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '5' DAY |

    @sail-bug
    Scenario: abs zero INTERVAL DAY
      When query
        """
        SELECT abs(INTERVAL '0' DAY) AS result
        """
      Then query result
        | result           |
        | INTERVAL '0' DAY |

    Scenario: abs negative INTERVAL DAY TO SECOND
      When query
        """
        SELECT abs(INTERVAL '-1 02:03:04' DAY TO SECOND) AS result
        """
      Then query result
        | result                              |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

    @sail-bug
    Scenario: abs negative INTERVAL HOUR TO MINUTE
      When query
        """
        SELECT abs(INTERVAL '-1:30' HOUR TO MINUTE) AS result
        """
      Then query result
        | result                         |
        | INTERVAL '01:30' HOUR TO MINUTE |

  Rule: Interval overflow always errors (regardless of ANSI mode)
    # Spark errors with ARITHMETIC_OVERFLOW on abs(interval_MIN) UNCONDITIONALLY
    # — interval abs is always-checked, unlike integer abs which respects
    # spark.sql.ansi.enabled. Verified against Spark JVM 4.x on 2026-04-25:
    # both ANSI=true and ANSI=false raise ARITHMETIC_OVERFLOW for the MIN of
    # both INTERVAL YEAR TO MONTH (i32::MIN months) and INTERVAL DAY TO SECOND
    # (i64::MIN microseconds). The MIN values must be constructed via
    # subtraction since literal parsers reject them.

    Scenario: abs INTERVAL YEAR TO MONTH MIN errors under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(INTERVAL '0' MONTH - INTERVAL '2147483647' MONTH - INTERVAL '1' MONTH) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

    Scenario: abs INTERVAL YEAR TO MONTH MIN errors under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(INTERVAL '0' MONTH - INTERVAL '2147483647' MONTH - INTERVAL '1' MONTH) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

    Scenario: abs INTERVAL DAY TO SECOND MIN errors under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(INTERVAL '0' MICROSECOND - INTERVAL '9223372036854775807' MICROSECOND - INTERVAL '1' MICROSECOND) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

    Scenario: abs INTERVAL DAY TO SECOND MIN errors under ANSI=true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(INTERVAL '0' MICROSECOND - INTERVAL '9223372036854775807' MICROSECOND - INTERVAL '1' MICROSECOND) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

  Rule: Multi-row vectorized path

    Scenario: abs BIGINT column with mixed signs and NULL
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES
          (CAST(-7 AS BIGINT)),
          (CAST(0 AS BIGINT)),
          (CAST(42 AS BIGINT)),
          (CAST(NULL AS BIGINT)),
          (CAST(-100 AS BIGINT))
        AS t(v)
        """
      Then query result
        | result |
        | 7      |
        | 0      |
        | 42     |
        | NULL   |
        | 100    |

    Scenario: abs DOUBLE column with mixed signs and NULL
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES
          (CAST(-1.5 AS DOUBLE)),
          (CAST(0.0 AS DOUBLE)),
          (CAST(3.25 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST(-99.75 AS DOUBLE))
        AS t(v)
        """
      Then query result
        | result |
        | 1.5    |
        | 0.0    |
        | 3.25   |
        | NULL   |
        | 99.75  |

    Scenario: abs INT column with NULL mix
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES (-5), (0), (5), (CAST(NULL AS INT)), (CAST(-2147483648 AS INT)) AS t(v)
        """
      Then query result
        | result      |
        | 5           |
        | 0           |
        | 5           |
        | NULL        |
        | -2147483648 |

    @sail-bug
    # Vectorized abs path is correct — same root cause as the scalar interval
    # scenarios in `Rule: Interval values`: Sail widens Spark interval subranges
    # to DAY TO SECOND at the type layer. The vectorized kernel succeeds; the
    # rendered/expected interval format diverges.
    # Fix path: preserve subrange in Spark→Arrow Field metadata.
    Scenario: abs INTERVAL DAY column with NULL mix
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES
          (INTERVAL '-5' DAY),
          (INTERVAL '0' DAY),
          (INTERVAL '10' DAY),
          (CAST(NULL AS INTERVAL DAY))
        AS t(v)
        """
      Then query result
        | result             |
        | INTERVAL '5' DAY   |
        | INTERVAL '0' DAY   |
        | INTERVAL '10' DAY  |
        | NULL               |

  Rule: All-null short-circuit
    # When every input row is NULL, invoke returns an all-null result
    # without running the kernel. Coverage on integer + interval paths
    # (floats/decimals are delegated to DataFusion's abs).

    Scenario: all-null INT column returns all NULL under ANSI=false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: all-null INT column returns all NULL under ANSI=true (no overflow check fires)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

    Scenario: all-null INTERVAL column returns all NULL
      When query
        """
        SELECT abs(v) AS result
        FROM VALUES (CAST(NULL AS INTERVAL DAY)), (CAST(NULL AS INTERVAL DAY)) AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: Type rejection

    Scenario: abs on BOOLEAN errors
      When query
        """
        SELECT abs(true) AS result
        """
      Then query error .*

    Scenario: abs on DATE errors
      When query
        """
        SELECT abs(DATE '2024-01-15') AS result
        """
      Then query error .*

    Scenario: abs on TIMESTAMP errors
      When query
        """
        SELECT abs(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query error .*

    Scenario: abs on BINARY errors
      When query
        """
        SELECT abs(X'48656C6C6F') AS result
        """
      Then query error .*

    Scenario: abs on ARRAY errors
      When query
        """
        SELECT abs(array(1,2,3)) AS result
        """
      Then query error .*

    Scenario: abs on MAP errors
      When query
        """
        SELECT abs(map('a',1)) AS result
        """
      Then query error .*

    Scenario: abs on STRUCT errors
      When query
        """
        SELECT abs(named_struct('a',1)) AS result
        """
      Then query error .*

  Rule: cross-nesting result correctness

    Scenario: ceil of abs for INT returns correct values
      When query
        """
        SELECT ceil(abs(v)) AS result
        FROM VALUES (CAST(-5 AS INT)), (CAST(0 AS INT)), (CAST(5 AS INT))
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result |
        | 0      |
        | 5      |
        | 5      |

    Scenario: ceil of abs for BIGINT returns correct values
      When query
        """
        SELECT ceil(abs(v)) AS result
        FROM VALUES (CAST(-5 AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(5 AS BIGINT))
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result |
        | 0      |
        | 5      |
        | 5      |

  Rule: simplify rewrite shape
    # Locks the simplify hook's dispatch (Int8/Int16/Int32/Int64/Interval/Duration
    # stay in SparkAbs invoke for ANSI handling; floats/decimals/null delegate
    # to DataFusion's abs). If a future refactor changes either branch, the
    # snapshot diff signals the hook is no longer firing as designed.

    Scenario: EXPLAIN abs INT column keeps spark_abs (ANSI path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-5 AS INT)), (CAST(0 AS INT)), (CAST(5 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs BIGINT column keeps spark_abs (ANSI path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-5 AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(5 AS BIGINT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs DOUBLE column delegates to DataFusion abs
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-1.5 AS DOUBLE)), (CAST(0.0 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs DECIMAL column delegates to DataFusion abs
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (CAST(-1.50 AS DECIMAL(5,2))), (CAST(1.50 AS DECIMAL(5,2))) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs INTERVAL column keeps spark_abs (kernel path retained)
      When query
        """
        EXPLAIN SELECT abs(v) FROM VALUES (INTERVAL '-5' DAY), (INTERVAL '5' DAY) AS t(v)
        """
      Then query plan matches snapshot

  @sail-only
  Rule: cross-nesting with other UDFs
    # Verifies abs simplify/output_ordering composes correctly with other
    # planner hooks already on main (e.g. ceil/floor).

    Scenario: EXPLAIN abs of ceil keeps spark_abs delegation chain consistent
      When query
        """
        EXPLAIN SELECT abs(ceil(v)) FROM VALUES (CAST(-1.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN ceil of abs keeps abs in invoke for integer path
      When query
        """
        EXPLAIN SELECT ceil(abs(v)) FROM VALUES (CAST(-5 AS INT)), (CAST(5 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

  @sail-only
  Rule: constant folding (DataFusion EvaluateScalarsAsConst)
    # Locks DataFusion's general optimizer behavior on constant inputs.
    # These folds happen via DataFusion's EvaluateScalarsAsConst rule
    # (NOT via SparkAbs::simplify) — our hook returns Original for
    # integers but DF then evaluates the constant via invoke_with_args
    # at planning time.

    Scenario: EXPLAIN abs of NULL folds to NULL literal
      When query
        """
        EXPLAIN SELECT abs(NULL) AS result
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs of negative INT literal folds to positive
      When query
        """
        EXPLAIN SELECT abs(CAST(-5 AS INT)) AS result
        """
      Then query plan matches snapshot

  @sail-only
  Rule: idempotence simplify (abs(abs(x)) = abs(x))
    # SparkAbs::simplify detects nested abs calls and collapses them.
    # Uses downcast_ref::<Self>() + ansi_mode check so only same-mode
    # abs chains collapse (e.g. ANSI abs(ANSI abs(x)) = ANSI abs(x)).
    # DataFusion applies simplify bottom-up to a fixed point, so
    # abs(abs(abs(x))) collapses in two passes without special-casing.

    Scenario: EXPLAIN abs(abs(int_col)) collapses to single spark_abs
      When query
        """
        EXPLAIN SELECT abs(abs(v)) FROM VALUES (CAST(-3 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN triple-nested abs collapses to single spark_abs
      When query
        """
        EXPLAIN SELECT abs(abs(abs(v))) FROM VALUES (CAST(-3 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN abs(abs(double_col)) does NOT collapse (DataFusion abs lacks idempotence)
      # Bottom-up simplification: inner SparkAbs(double) becomes df_abs first.
      # Outer SparkAbs sees df_abs as args[0] — name mismatch ("spark_abs" != "abs"),
      # so idempotence doesn't fire. Then outer type-dispatches Double → df_abs again.
      # Net result: df_abs(df_abs(double)). Would require upstream contribution to
      # DataFusion's abs to implement its own idempotence.
      When query
        """
        EXPLAIN SELECT abs(abs(v)) FROM VALUES (CAST(-1.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    Scenario: abs(abs(x)) returns same as abs(x) (correctness — both ANSI modes)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(abs(v)) AS result
        FROM VALUES (-5), (0), (5), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result
        | result |
        | 5      |
        | 0      |
        | 5      |
        | NULL   |

    Scenario: abs(abs(INT_MIN)) under ANSI=true errors at inner abs
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(abs(CAST(-2147483648 AS INT))) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*
        | 5      |
