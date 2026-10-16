Feature: bround comprehensive tests
  # bround = banker's rounding (round-half-to-even). The scenarios below use
  # explicit CAST to DOUBLE/FLOAT/INT/BIGINT so they exercise the vectorized
  # PrimitiveArray paths (Float64/Float32/Int32/Int64) rather than the scalar
  # Decimal128 fast path. All values validated against Spark JVM 4.x.

  Rule: Double tie-to-even (round-half-to-even) at scale 0

    Scenario Outline: Scale 0: <case>
      When query
        """
        SELECT bround(CAST(<v> AS DOUBLE), 0) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | v    | result |
        | bround DOUBLE 2.5 rounds down to even | 2.5  | 2.0    |
        | bround DOUBLE 3.5 rounds up to even   | 3.5  | 4.0    |
        | bround DOUBLE -2.5 rounds to even     | -2.5 | -2.0   |
        | bround DOUBLE -3.5 rounds to even     | -3.5 | -4.0   |
        | bround DOUBLE 2.4 no tie rounds down  | 2.4  | 2.0    |
        | bround DOUBLE 2.6 no tie rounds up    | 2.6  | 3.0    |

  Rule: Double tie-to-even at positive scale

    Scenario Outline: Positive scale: <case>
      When query
        """
        SELECT bround(CAST(<v> AS DOUBLE), 1) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | v    | result |
        | bround DOUBLE 1.25 scale 1 | 1.25 | 1.2    |
        | bround DOUBLE 1.35 scale 1 | 1.35 | 1.4    |

  Rule: Integer paths with negative scale preserve input type

    Scenario Outline: Integer negative scale: <case>
      When query
        """
        SELECT bround(CAST(<v> AS <type>), -1) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | v  | type   | result |
        | bround INT 25 scale -1    | 25 | INT    | 20     |
        | bround INT 35 scale -1    | 35 | INT    | 40     |
        | bround BIGINT 25 scale -1 | 25 | BIGINT | 20     |

  Rule: NULL propagation

    Scenario: bround NULL DOUBLE value
      When query
        """
        SELECT bround(CAST(NULL AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: bround NULL foldable scale yields NULL
      When query
        """
        SELECT bround(CAST(2.5 AS DOUBLE), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row vectorized path with NULL mix
    # These exercise the binary-kernel null-buffer intersection directly:
    # the output is NULL exactly where the value is NULL.

    Scenario: bround DOUBLE column with NULL mix
      When query
        """
        SELECT bround(v, 0) AS result
        FROM VALUES
          (CAST(2.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST(3.5 AS DOUBLE)),
          (CAST(-2.5 AS DOUBLE))
        AS t(v)
        """
      Then query result ordered
        | result |
        | 2.0    |
        | NULL   |
        | 4.0    |
        | -2.0   |

    Scenario: bround INT column with NULL mix
      When query
        """
        SELECT bround(v, -1) AS result
        FROM VALUES
          (CAST(25 AS INT)),
          (CAST(NULL AS INT)),
          (CAST(35 AS INT))
        AS t(v)
        """
      Then query result ordered
        | result |
        | 20     |
        | NULL   |
        | 40     |

  Rule: Special values NaN and Infinity pass through unchanged (DOUBLE)
    # bround passes NaN / +Infinity / -Infinity straight through regardless of
    # the scale argument (verified against Spark JVM at scale 0, 2, -1). The
    # binary kernel applies the closure per value; the closure's round path is a
    # no-op on non-finite inputs.

    Scenario Outline: Special value: <case>
      When query
        """
        SELECT bround(CAST(<v> AS DOUBLE), <scale>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | v           | scale | result    |
        | bround DOUBLE NaN scale 0               | 'NaN'       | 0     | NaN       |
        | bround DOUBLE Infinity scale 0          | 'Infinity'  | 0     | Infinity  |
        | bround DOUBLE negative Infinity scale 0 | '-Infinity' | 0     | -Infinity |
        | bround DOUBLE NaN positive scale        | 'NaN'       | 2     | NaN       |
        | bround DOUBLE Infinity negative scale   | 'Infinity'  | -1    | Infinity  |

  Rule: Multi-row DOUBLE with NULL, NaN and Infinity mix
    # Exercises the binary-kernel null-buffer intersection alongside non-finite
    # values in a single vectorized call: NULL stays NULL, NaN/±Infinity pass
    # through, finite ties round half-to-even.

    Scenario: bround DOUBLE column with NULL NaN and Infinity mix
      When query
        """
        SELECT bround(v, 0) AS result
        FROM VALUES
          (CAST(2.5 AS DOUBLE)),
          (CAST(NULL AS DOUBLE)),
          (CAST('NaN' AS DOUBLE)),
          (CAST('Infinity' AS DOUBLE)),
          (CAST('-Infinity' AS DOUBLE)),
          (CAST(-2.5 AS DOUBLE))
        AS t(v)
        """
      Then query result ordered
        | result    |
        | 2.0       |
        | NULL      |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | -2.0      |

    @sail-bug
    # Pre-existing Float32 mismatch: `return_field_from_args` promises Float64
    # (spark_bround.rs:69-72) while the Float32 branch yields a Float32Array
    # (spark_bround.rs:141-148). The special-value semantics themselves match Spark;
    # the declared type is what diverges, asserted directly by "a FLOAT input keeps
    # FLOAT" under `Rule: Output schema`.
    # This scenario asserts VALUES, so unlike that one it executes: what makes it
    # fail is the type check in datafusion-expr/src/udf.rs:277, which is
    # `#[cfg(debug_assertions)]`. In a release build it may instead fail on Arrow's
    # RecordBatch column-type validation, or not at all — and `@sail-bug` is
    # xfail(strict=True), so an XPASS turns CI red. Not measured against release.
    Scenario: bround FLOAT column with NULL NaN and Infinity mix
      When query
        """
        SELECT bround(v, 0) AS result
        FROM VALUES
          (CAST(2.5 AS FLOAT)),
          (CAST(NULL AS FLOAT)),
          (CAST('NaN' AS FLOAT)),
          (CAST('Infinity' AS FLOAT)),
          (CAST('-Infinity' AS FLOAT)),
          (CAST(-2.5 AS FLOAT))
        AS t(v)
        """
      Then query result ordered
        | result    |
        | 2.0       |
        | NULL      |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | -2.0      |

  Rule: Negative scale over a column (migrated from test_bround.txt)
    # Migrated from the former python/pysail/tests/spark/function/test_bround.txt
    # doctest: bround(n, -1) over an INT column and over a DOUBLE column,
    # preserving each input type.

    Scenario Outline: Column negative scale: <case>
      When query
        """
        SELECT bround(n, -1) AS result
        FROM VALUES <values> AS t(n)
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                          | values                                         | r1   | r2   |
        | bround INT column scale -1    | (25), (35)                                     | 20   | 40   |
        | bround DOUBLE column scale -1 | (CAST(25.0 AS DOUBLE)), (CAST(35.0 AS DOUBLE)) | 20.0 | 40.0 |

  Rule: bround — the argument must be foldable

    @function(columnargs)
    Scenario: bround with the argument as a literal
      When query
        """
        SELECT bround(25, -1) AS result
        """
      Then query result ordered
        | result |
        | 20     |

    # Spark requires a foldable argument here; Sail accepts a column and returns
    # a value per row instead of raising.
    @function(columnargs) @sail-bug
    Scenario Outline: Bround: <case>
      When query
        """
        SELECT bround(25, c) AS result FROM VALUES (1, -1), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

      Examples:
        | case                                                               | v2 |
        | bround takes argument 2 from a column holding two different values | 0  |
        | bround takes argument 2 from a column                              | -1 |

    @sail-bug
    @function(columnargs)
    Scenario: bround takes argument 2 from a column holding two different values
      When query
        """
        SELECT bround(25, c) AS result FROM VALUES (1, -1), (2, 0) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    @sail-bug
    @function(columnargs)
    Scenario: bround takes argument 2 from a column
      When query
        """
        SELECT bround(25, c) AS result FROM VALUES (1, -1), (2, -1) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null numeric literal is nullable (inherently nullable in Spark)
      When query
        """
        SELECT bround(2.5, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(2,0) (nullable = true)
        """

    Scenario: a non-null numeric column is nullable (inherently nullable in Spark)
      When query
        """
        SELECT bround(id, 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable numeric column stays nullable
      When query
        """
        SELECT bround(c, 1) AS result FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    # `RoundBase.dataType = child.dataType`, with a precision/scale adjustment for decimals
    # (mathExpressions.scala:1567-1584, `case t => t` at :1583). Spark inserts no cast for any
    # of these: `inputTypes = Seq(NumericType, IntegerType)` (:1586) and `NumericType` accepts
    # FLOAT, DECIMAL, TINYINT and SMALLINT directly.
    #
    # These assert the output data TYPE, not nullability, so they inherit
    # `@function(nullability)` from this Rule without being about it — filed here deliberately
    # rather than in a Rule of their own, because `Rule: Output schema` is where the declared
    # field lives. Asserted on the schema rather than on a value because the declared type comes
    # from `return_field_from_args` at plan time, so it is identical in debug and release; the
    # runtime type check is debug-only (datafusion-expr/src/udf.rs:277).
    #
    # Two DIFFERENT Sail bugs are recorded below, despite the shared Rule:
    #   - FLOAT and DECIMAL: the type is declared wrong (`Float64`, spark_bround.rs:69-72).
    #   - TINYINT and SMALLINT: the input is REJECTED outright — `Int8`/`Int16` have no arm in
    #     `return_field_from_args` and fall through to `unsupported_data_type_exec_err`. The
    #     `coerce_types` that would widen them (spark_bround.rs:191) is dead code, because the
    #     signature is `variadic_any` and DataFusion only consults `coerce_types` for
    #     `TypeSignature::UserDefined`. So fixing type propagation alone will NOT turn these two
    #     green, and `strict=True` will not flag the tags as outdated either.

    @sail-bug
    Scenario: a FLOAT input keeps FLOAT
      When query
        """
        SELECT bround(v, 0) AS result FROM VALUES (CAST(2.5 AS FLOAT)) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

    @sail-bug
    Scenario: a DECIMAL input keeps an adjusted DECIMAL
      When query
        """
        SELECT bround(v, 2) AS result FROM VALUES (CAST(1.005 AS DECIMAL(10,3))) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: decimal(10,2) (nullable = true)
        """

    @sail-bug
    Scenario: a TINYINT input keeps TINYINT
      When query
        """
        SELECT bround(v, 0) AS result FROM VALUES (CAST(1 AS TINYINT)) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = true)
        """

    @sail-bug
    Scenario: a SMALLINT input keeps SMALLINT
      When query
        """
        SELECT bround(v, 0) AS result FROM VALUES (CAST(1 AS SMALLINT)) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: short (nullable = true)
        """

  Rule: bround rounds half to even on DECIMAL too, not only on DOUBLE

    # A bare `2.5` is a DECIMAL in Spark, and this is where bround earns its name. Sail rounds
    # half UP on the decimal path -- that is `round`, not `bround` -- and only gets the DOUBLE path
    # right, which is why the scenarios above (all of them CAST to DOUBLE) never caught it.

    # Sail returns 3.0.
    @sail-bug
    Scenario: bround DECIMAL 2.5 rounds down to even
      When query
        """
        SELECT bround(2.5) AS result
        """
      Then query result
        | result |
        | 2      |

    # Sail returns 1.0.
    @sail-bug
    Scenario: bround DECIMAL 0.5 rounds down to even
      When query
        """
        SELECT bround(0.5) AS result
        """
      Then query result
        | result |
        | 0      |

    # Sail returns -3.0.
    @sail-bug
    Scenario: bround DECIMAL -2.5 rounds up to even
      When query
        """
        SELECT bround(-2.5) AS result
        """
      Then query result
        | result |
        | -2     |

  Rule: the result keeps the type of the value

    # Spark's bround returns the input's type; Sail casts everything to DOUBLE. This is the root of
    # most of the value divergences above: the number is right, the type is not.

    # Sail returns double.
    @sail-bug
    Scenario: bround of a DECIMAL returns a DECIMAL
      When query
        """
        SELECT bround(CAST(2.5 AS DECIMAL(10,2)), 1) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(10,1) (nullable = true)
        """

    # Sail errors: Assertion failed: result_data_type == *expected_type.
    @sail-bug
    Scenario: bround of a FLOAT returns a FLOAT
      When query
        """
        SELECT bround(CAST(2.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 2.0    |

  Rule: bround accepts every numeric type, a string, and NULL

    # Sail errors: Unsupported Data Type: Spark `spark_bround` function expects Float64, Float32...

    @sail-bug
    Scenario: bround of a TINYINT
      When query
        """
        SELECT bround(CAST(25 AS TINYINT), -1) AS result
        """
      Then query result
        | result |
        | 20     |

    @sail-bug
    Scenario: bround of a SMALLINT
      When query
        """
        SELECT bround(CAST(25 AS SMALLINT), -1) AS result
        """
      Then query result
        | result |
        | 20     |

    @sail-bug
    Scenario: bround of a string casts it to a double
      When query
        """
        SELECT bround('2.5') AS result
        """
      Then query result
        | result |
        | 2.0    |

    @sail-bug
    Scenario: bround of an untyped NULL is NULL
      When query
        """
        SELECT bround(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: bround with a NULL scale is NULL
      When query
        """
        SELECT bround(1.5, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    # The columnar kernel, not the constant-folded scalar path. Sail errors:
    # Unsupported Data Type: Spark `spark_bround` function expects vectorized Decimal...
    @sail-bug
    Scenario: bround of a DECIMAL column
      When query
        """
        SELECT bround(v, 0) AS result FROM VALUES (1, 2.5), (2, 3.5), (3, -2.5), (4, CAST(NULL AS DECIMAL(3,1))) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2      |
        | 4      |
        | -2     |
        | NULL   |

  Rule: rounding that overflows the type errors under ANSI, and wraps otherwise

    # Sail neither errors nor wraps: it widens to a double and returns 130.0.
    @sail-bug
    Scenario: bround overflowing a TINYINT errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bround(CAST(127 AS TINYINT), -1) AS result
        """
      Then query error (?i)overflow

    @sail-bug
    Scenario: bround overflowing a TINYINT wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bround(CAST(127 AS TINYINT), -1) AS result
        """
      Then query result
        | result |
        | -126   |

  Rule: the value may come from a column

    # The literal scenarios are constant-folded and never reach the columnar kernel, which is in
    # worse shape than the scalar one: it rejects DECIMAL and asserts on FLOAT.

    # Sail errors: Assertion failed: result_data_type == *expected_type.
    @sail-bug
    Scenario: bround of a FLOAT column keeps the FLOAT type
      When query
        """
        SELECT bround(v, 0) AS result FROM VALUES (1, CAST(2.5 AS FLOAT)), (2, CAST(3.5 AS FLOAT)) AS t(i, v) ORDER BY i
        """
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """
      Then query result ordered
        | result |
        | 2.0    |
        | 4.0    |

    # Sail errors: Unsupported Data Type: Spark `spark_bround` function expects Float64, Float32...
    @sail-bug
    # Note 25 rounds DOWN to 20: half-even takes 2.5 to the even 2, where `round` would take it to 3.
    Scenario: bround of a TINYINT column
      When query
        """
        SELECT bround(CAST(v AS TINYINT), -1) AS result FROM VALUES (1, 25), (2, 24) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result |
        | 20     |
        | 20     |

    # Sail errors: Unsupported Data Type.
    @sail-bug
    Scenario: bround of a string column
      When query
        """
        SELECT bround(v) AS result FROM VALUES (1, '2.5'), (2, '3.5') AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2.0    |
        | 4.0    |

    # Sail returns 2147483647 without erroring.
    @sail-bug
    Scenario: bround of an INT column that overflows errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bround(v, -1) AS result FROM VALUES (1, 2147483647), (2, 1) AS t(i, v) ORDER BY i
        """
      Then query error (?i)overflow

  Rule: Decimal literal tie-to-even (half-even, not half-up)
    # Regression: the Decimal128 scalar fast path used `f64::round()`
    # (half-away-from-zero) instead of round-half-to-even, so `bround(2.5, 0)`
    # returned 3.0 instead of 2.0. Bare decimal literals (2.5, 3.5, ...) route
    # through that path. Result is CAST to DOUBLE to normalise the return type
    # (Sail's decimal path yields DOUBLE, Spark yields DECIMAL) so the scenario
    # checks the VALUE, not the type — both render 2.0.

    @sail-bug
    Scenario: bround decimal literal 2.5 rounds to even
      When query
        """
        SELECT CAST(bround(2.5, 0) AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: bround decimal literal 3.5 rounds to even
      When query
        """
        SELECT CAST(bround(3.5, 0) AS DOUBLE) AS result
        """
      Then query result
        | result |
        | 4.0    |

    @sail-bug
    Scenario: bround decimal literal -2.5 rounds to even
      When query
        """
        SELECT CAST(bround(-2.5, 0) AS DOUBLE) AS result
        """
      Then query result
        | result |
        | -2.0   |

  Rule: High-scale rounding matches Spark's exact decimal rounding
    # Spark rounds the EXACT decimal value of the double
    # (`BigDecimal(d).setScale(scale, HALF_EVEN)`). Naive binary rounding
    # (`(x * 10^scale).round_ties_even() / 10^scale`) loses the sub-ulp tail of
    # the product, so a value just past the .5 boundary in exact decimal lands
    # on .5 in binary and ties the other way — a 1-ulp divergence that appears
    # from scale 6 up (0 at scale ≤ 5). For scale 6..=22 Sail switches to an
    # FMA-compensated round that recovers the exact product tail and is
    # bit-identical to Spark (verified over 200M+ samples).

    Scenario: bround DOUBLE positive value at scale 6
      When query
        """
        SELECT bround(CAST(741.6236645 AS DOUBLE), 6) AS result
        """
      Then query result
        | result     |
        | 741.623664 |

    Scenario: bround DOUBLE negative value at scale 6
      When query
        """
        SELECT bround(CAST(-6904.8782075 AS DOUBLE), 6) AS result
        """
      Then query result
        | result       |
        | -6904.878208 |
