@bround
Feature: bround comprehensive tests
  # bround = banker's rounding (round-half-to-even). The scenarios below use
  # explicit CAST to DOUBLE/FLOAT/INT/BIGINT so they exercise the vectorized
  # PrimitiveArray paths (Float64/Float32/Int32/Int64) rather than the scalar
  # Decimal128 fast path. All values validated against Spark JVM 4.1.1.

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
