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
