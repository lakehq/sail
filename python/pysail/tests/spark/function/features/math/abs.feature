Feature: abs comprehensive tests

  Rule: Argument count validation

    Scenario Outline: Arity: <case>
      When query
        """
        SELECT abs(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                 | args |
        | abs zero args errors |      |
        | abs two args errors  | 1, 2 |

  Rule: NULL propagation

    Scenario Outline: abs of a NULL input propagates NULL
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | input                                |
        | NULL                                 |
        | CAST(NULL AS INT)                    |
        | CAST(NULL AS TINYINT)                |
        | CAST(NULL AS SMALLINT)               |
        | CAST(NULL AS BIGINT)                 |
        | CAST(NULL AS FLOAT)                  |
        | CAST(NULL AS DOUBLE)                 |
        | CAST(NULL AS DECIMAL(10,2))          |
        | CAST(NULL AS INTERVAL DAY TO SECOND) |

  Rule: Basic integer types happy path

    Scenario Outline: abs on basic integer values
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input                                | result              |
        | -5                                   | 5                   |
        | 5                                    | 5                   |
        | 0                                    | 0                   |
        | CAST(-127 AS TINYINT)                | 127                 |
        | CAST(-32767 AS SMALLINT)             | 32767               |
        | CAST(-9223372036854775807 AS BIGINT) | 9223372036854775807 |

  Rule: Float and double values

    Scenario Outline: abs on float and double values
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input                       | result   |
        | CAST(-1.5 AS DOUBLE)        | 1.5      |
        | CAST(1.5 AS FLOAT)          | 1.5      |
        | CAST(-0.0 AS DOUBLE)        | 0.0      |
        | CAST(-0.0 AS FLOAT)         | 0.0      |
        | CAST('NaN' AS DOUBLE)       | NaN      |
        | CAST('Infinity' AS DOUBLE)  | Infinity |
        | CAST('-Infinity' AS DOUBLE) | Infinity |

  Rule: Decimal values

    Scenario Outline: abs on decimal values
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input                         | result |
        | CAST(-1.5 AS DECIMAL(5,2))    | 1.50   |
        | CAST(0 AS DECIMAL(10,2))      | 0.00   |
        | CAST(-0.001 AS DECIMAL(10,3)) | 0.001  |

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
        | result                                 |
        | 10000000000000000000000000000000000000 |

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

    Scenario Outline: abs of typed MIN wraps to MIN under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input                                | result               |
        | CAST(-128 AS TINYINT)                | -128                 |
        | CAST(-32768 AS SMALLINT)             | -32768               |
        | CAST(-2147483648 AS INT)             | -2147483648          |
        | CAST(-9223372036854775808 AS BIGINT) | -9223372036854775808 |

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

    Scenario Outline: abs of typed MIN errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

      Examples:
        | input                                |
        | CAST(-128 AS TINYINT)                |
        | CAST(-2147483648 AS INT)             |
        | CAST(-9223372036854775808 AS BIGINT) |

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

    Scenario Outline: abs of a parseable numeric string under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | input | result |
        | '-5'  | 5.0    |
        | '5.5' | 5.5    |
        | 'NaN' | NaN    |

    @sail-bug
    Scenario Outline: String coercion under ANSI=false: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | input    | result |
        | abs whitespace-padded numeric string                 | '  -5  ' | 5.0    |
        | abs non-numeric string returns NULL under ANSI false | 'hello'  | NULL   |
        | abs empty string returns NULL under ANSI false       | ''       | NULL   |

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
    Scenario Outline: Interval subrange: <case>
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | input                           | result                          |
        | abs negative INTERVAL DAY            | INTERVAL '-5' DAY               | INTERVAL '5' DAY                |
        | abs positive INTERVAL DAY            | INTERVAL '5' DAY                | INTERVAL '5' DAY                |
        | abs zero INTERVAL DAY                | INTERVAL '0' DAY                | INTERVAL '0' DAY                |
        | abs negative INTERVAL HOUR TO MINUTE | INTERVAL '-1:30' HOUR TO MINUTE | INTERVAL '01:30' HOUR TO MINUTE |

    Scenario: abs negative INTERVAL DAY TO SECOND
      When query
        """
        SELECT abs(INTERVAL '-1 02:03:04' DAY TO SECOND) AS result
        """
      Then query result
        | result                              |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

  Rule: Interval overflow always errors (regardless of ANSI mode)
    # Spark errors with ARITHMETIC_OVERFLOW on abs(interval_MIN) UNCONDITIONALLY
    # — interval abs is always-checked, unlike integer abs which respects
    # spark.sql.ansi.enabled. Verified against Spark JVM 4.x on 2026-04-25:
    # both ANSI=true and ANSI=false raise ARITHMETIC_OVERFLOW for the MIN of
    # both INTERVAL YEAR TO MONTH (i32::MIN months) and INTERVAL DAY TO SECOND
    # (i64::MIN microseconds). The MIN values must be constructed via
    # subtraction since literal parsers reject them.

    Scenario Outline: abs of interval MIN errors regardless of ANSI mode
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query error .*\[ARITHMETIC_OVERFLOW\].*

      Examples:
        | ansi  | input                                                                                            |
        | false | INTERVAL '0' MONTH - INTERVAL '2147483647' MONTH - INTERVAL '1' MONTH                            |
        | true  | INTERVAL '0' MONTH - INTERVAL '2147483647' MONTH - INTERVAL '1' MONTH                            |
        | false | INTERVAL '0' MICROSECOND - INTERVAL '9223372036854775807' MICROSECOND - INTERVAL '1' MICROSECOND |
        | true  | INTERVAL '0' MICROSECOND - INTERVAL '9223372036854775807' MICROSECOND - INTERVAL '1' MICROSECOND |

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
        | result            |
        | INTERVAL '5' DAY  |
        | INTERVAL '0' DAY  |
        | INTERVAL '10' DAY |
        | NULL              |

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

    Scenario Outline: abs on an unsupported type errors
      When query
        """
        SELECT abs(<input>) AS result
        """
      Then query error .*

      Examples:
        | input                           |
        | true                            |
        | DATE '2024-01-15'               |
        | TIMESTAMP '2024-01-15 12:00:00' |
        | X'48656C6C6F'                   |
        | array(1,2,3)                    |
        | map('a',1)                      |
        | named_struct('a',1)             |

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

  @sail-only
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
        | 5 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null integer literal yields a non-nullable integer
      When query
        """
        SELECT abs(-5) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null integer column yields a non-nullable integer
      When query
        """
        SELECT abs(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable integer column stays nullable
      When query
        """
        SELECT abs(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
