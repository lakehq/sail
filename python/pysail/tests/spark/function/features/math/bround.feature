@bround
Feature: bround comprehensive tests
  # bround = banker's rounding (round-half-to-even). The scenarios below use
  # explicit CAST to DOUBLE/FLOAT/INT/BIGINT so they exercise the vectorized
  # PrimitiveArray paths (Float64/Float32/Int32/Int64) rather than the scalar
  # Decimal128 fast path. All values validated against Spark JVM 4.x.

  Rule: Double tie-to-even (round-half-to-even) at scale 0

    Scenario: bround DOUBLE 2.5 rounds down to even
      When query
        """
        SELECT bround(CAST(2.5 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: bround DOUBLE 3.5 rounds up to even
      When query
        """
        SELECT bround(CAST(3.5 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | 4.0    |

    Scenario: bround DOUBLE -2.5 rounds to even
      When query
        """
        SELECT bround(CAST(-2.5 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | -2.0   |

    Scenario: bround DOUBLE -3.5 rounds to even
      When query
        """
        SELECT bround(CAST(-3.5 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | -4.0   |

    Scenario: bround DOUBLE 2.4 no tie rounds down
      When query
        """
        SELECT bround(CAST(2.4 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | 2.0    |

    Scenario: bround DOUBLE 2.6 no tie rounds up
      When query
        """
        SELECT bround(CAST(2.6 AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | 3.0    |

  Rule: Double tie-to-even at positive scale

    Scenario: bround DOUBLE 1.25 scale 1
      When query
        """
        SELECT bround(CAST(1.25 AS DOUBLE), 1) AS result
        """
      Then query result
        | result |
        | 1.2    |

    Scenario: bround DOUBLE 1.35 scale 1
      When query
        """
        SELECT bround(CAST(1.35 AS DOUBLE), 1) AS result
        """
      Then query result
        | result |
        | 1.4    |

  # FIXME: The type mismatch assertion is only active in debug builds,
  #   so the following tests would fail with XPASS in release builds.
  #
  #   Rule: Float32 path
  #     # Pre-existing bug (NOT caused by the binary-kernel perf refactor, which
  #     # preserves the Float32Array output). `return_type` maps a FLOAT/Float32
  #     # input to Float64, but the Float32 execution branch yields a Float32Array,
  #     # so execution fails with a planning-vs-runtime type mismatch
  #     # ("type 'Float32' ... expected: 'Float64'"). Spark returns FLOAT here.
  #     # Fix path (separate PR): make return_type return Float32 for Float32 input
  #     # (or coerce the arg/result to Float64). Kept perf-only in this PR.
  #
  #     @sail-bug
  #     Scenario: bround FLOAT 2.5 rounds to even
  #       When query
  #         """
  #         SELECT bround(CAST(2.5 AS FLOAT), 0) AS result
  #         """
  #       Then query result
  #         | result |
  #         | 2.0    |
  #
  #     @sail-bug
  #     Scenario: bround FLOAT 3.5 rounds to even
  #       When query
  #         """
  #         SELECT bround(CAST(3.5 AS FLOAT), 0) AS result
  #         """
  #       Then query result
  #         | result |
  #         | 4.0    |
  #
  Rule: Integer paths with negative scale preserve input type

    Scenario: bround INT 25 scale -1
      When query
        """
        SELECT bround(CAST(25 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 20     |

    Scenario: bround INT 35 scale -1
      When query
        """
        SELECT bround(CAST(35 AS INT), -1) AS result
        """
      Then query result
        | result |
        | 40     |

    Scenario: bround BIGINT 25 scale -1
      When query
        """
        SELECT bround(CAST(25 AS BIGINT), -1) AS result
        """
      Then query result
        | result |
        | 20     |

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

    Scenario: bround DOUBLE NaN scale 0
      When query
        """
        SELECT bround(CAST('NaN' AS DOUBLE), 0) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: bround DOUBLE Infinity scale 0
      When query
        """
        SELECT bround(CAST('Infinity' AS DOUBLE), 0) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: bround DOUBLE negative Infinity scale 0
      When query
        """
        SELECT bround(CAST('-Infinity' AS DOUBLE), 0) AS result
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: bround DOUBLE NaN positive scale
      When query
        """
        SELECT bround(CAST('NaN' AS DOUBLE), 2) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: bround DOUBLE Infinity negative scale
      When query
        """
        SELECT bround(CAST('Infinity' AS DOUBLE), -1) AS result
        """
      Then query result
        | result   |
        | Infinity |

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
    # Same pre-existing Float32 return_type mismatch as the "Float32 path" rule
    # above (Float32 branch yields Float32Array while return_type promises
    # Float64). The special-value semantics themselves match Spark. Fix in the
    # separate return_type PR; kept perf-only here.
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

    Scenario: bround INT column scale -1
      When query
        """
        SELECT bround(n, -1) AS result
        FROM VALUES (25), (35) AS t(n)
        """
      Then query result ordered
        | result |
        | 20     |
        | 40     |

    Scenario: bround DOUBLE column scale -1
      When query
        """
        SELECT bround(n, -1) AS result
        FROM VALUES (CAST(25.0 AS DOUBLE)), (CAST(35.0 AS DOUBLE)) AS t(n)
        """
      Then query result ordered
        | result |
        | 20.0   |
        | 40.0   |

  Rule: bround — the argument must be foldable

    @column_args
    Scenario: bround with the argument as a literal
      When query
        """
        SELECT bround(25, -1) AS result
        """
      Then query result ordered
        | result |
        | 20     |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['20', '25'].
    @column_args @sail-bug
    Scenario: bround takes argument 2 from a column holding two different values
      When query
        """
        SELECT bround(25, c) AS result FROM VALUES (1, -1), (2, 0) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['20', '20'].
    @column_args @sail-bug
    Scenario: bround takes argument 2 from a column
      When query
        """
        SELECT bround(25, c) AS result FROM VALUES (1, -1), (2, -1) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  Rule: bround with a foldable scale expression

    # Spark accepts any foldable scale and keeps the decimal type; Sail folds the scale
    # too, but `SparkBRound` only implements `return_type`, which never sees the scale
    # VALUE — hence every decimal input is typed DOUBLE. Closing it needs the migration
    # to `return_field_from_args` (issue #1903), the same one `round` already went
    # through, plus a vectorized decimal kernel: the Decimal128 path is `unsupported`
    # today, which is why the value is computed in double as well.
    @sail-bug
    Scenario: bround keeps the decimal type with a foldable scale
      When query
        """
        SELECT bround(CAST(3.14159 AS DECIMAL(10,5)), 1 + 1) AS result,
               typeof(bround(CAST(3.14159 AS DECIMAL(10,5)), 1 + 1)) AS t
        """
      Then query result
        | result | t            |
        | 3.14   | decimal(8,2) |

  Rule: bround over DECIMAL input (all @sail-bug — see issue #1903)
    # Three separate defects, all measured against Spark JVM 4.1.1:
    #
    # 1. WRONG VALUE. The scalar decimal path converts to `f64` and rounds with Rust's
    #    `.round()`, which is half-away-from-zero — so `bround` behaves like `round` for
    #    decimal input, losing the half-to-EVEN semantics the function exists for.
    #    The float paths use `spark_bround_f64` and are correct; only decimal is not.
    # 2. WRONG TYPE. `return_type` only sees the argument TYPES, never the scale VALUE,
    #    so it cannot apply Spark's `(p - s + 1) + min(s, scale)` and maps every decimal
    #    to DOUBLE. Fixing it needs `return_field_from_args` (issue #1903).
    # 3. HARD FAILURE on a decimal COLUMN: the vectorized Decimal128 path is
    #    `unsupported`, so the query errors outright while Spark returns a value.
    #
    # Closing this needs a real decimal kernel (half-to-even over i128 with rescaling)
    # plus the `return_field_from_args` migration. It is `round`'s decimal typing again.

    @sail-bug
    Scenario: bround of a decimal rounds half to even
      # The defining behaviour of bround: 2.5 -> 2 and 3.5 -> 4, never 3.
      When query
        """
        SELECT bround(CAST(2.5 AS DECIMAL(2,1)), 0) AS r,
               typeof(bround(CAST(2.5 AS DECIMAL(2,1)), 0)) AS t
        """
      Then query result
        | r | t            |
        | 2 | decimal(2,0) |

    @sail-bug
    Scenario: bround of a decimal with no scale rounds half to even
      When query
        """
        SELECT bround(CAST(2.5 AS DECIMAL(2,1))) AS r
        """
      Then query result
        | r |
        | 2 |

    @sail-bug
    Scenario: bround of a decimal column rounds half to even over rows
      # A decimal column errors today ("vectorized Decimal128" is unsupported).
      When query
        """
        SELECT bround(v, 0) AS r
        FROM VALUES (CAST(2.5 AS DECIMAL(2,1))), (CAST(3.5 AS DECIMAL(2,1))) AS t(v)
        """
      Then query result
        | r |
        | 2 |
        | 4 |

    @sail-bug
    Scenario: bround of a decimal column keeps the decimal type
      When query
        """
        SELECT bround(v, 2) AS r, typeof(bround(v, 2)) AS t
        FROM VALUES (CAST(3.14159 AS DECIMAL(10,5))) AS t(v)
        """
      Then query result
        | r    | t            |
        | 3.14 | decimal(8,2) |

    @sail-bug
    Scenario: bround of a decimal column with a negative scale
      When query
        """
        SELECT bround(v, -1) AS r, typeof(bround(v, -1)) AS t
        FROM VALUES (CAST(25 AS DECIMAL(10,0))) AS t(v)
        """
      Then query result
        | r  | t             |
        | 20 | decimal(11,0) |

    @sail-bug
    Scenario: bround of a wide decimal keeps every digit
      # Going through f64 loses the last digits: 0.123456789012345678 comes back as
      # 0.12345678901234568.
      When query
        """
        SELECT bround(CAST('0.123456789012345678' AS DECIMAL(38,18)), 18) AS r
        """
      Then query result
        | r                   |
        | 0.123456789012345678 |

  @spark_null
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
