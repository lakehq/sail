@datasketches
Feature: DataSketches functions

  Rule: HLL sketch functions estimate distinct values

    Scenario: hll_sketch_agg estimates distinct integer values
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (1), (1), (2), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: hll_union merges two sketches
      When query
        """
        SELECT hll_sketch_estimate(hll_union(hll_sketch_agg(col1), hll_sketch_agg(col2))) AS result
        FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 6      |

    Scenario: HLL scalar sketch functions accept untyped nulls
      When query
        """
        SELECT
          hll_sketch_estimate(NULL) AS estimate,
          hll_union(NULL, hll_sketch_agg(col)) IS NULL AS left_null,
          hll_union(hll_sketch_agg(col), NULL) IS NULL AS right_null,
          hll_union(hll_sketch_agg(col), hll_sketch_agg(col), NULL) IS NULL AS config_null
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | estimate | left_null | right_null | config_null |
        | NULL     | true      | true       | true        |

    Scenario: hll_union_agg merges sketch rows with different lgConfigK values when allowed
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch, true)) AS result
        FROM (
          SELECT hll_sketch_agg(col) AS sketch FROM VALUES (1) AS tab(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 20) AS sketch FROM VALUES (1) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 1      |

    Scenario: HLL sketch aggregates accept untyped null sketch inputs
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(NULL)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: hll_union_agg empty result matches Spark union lgConfigK
      When query
        """
        SELECT hll_sketch_estimate(hll_union(hll_union_agg(NULL), hll_sketch_agg(col, 12), false)) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | result |
        | 1      |

    Scenario: hll_union_agg empty result rejects different lgConfigK
      When query
        """
        SELECT hll_sketch_estimate(hll_union(hll_union_agg(NULL), hll_sketch_agg(col, 21), false)) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query error (HLL_UNION_DIFFERENT_LG_K|different lgConfigK)

    Scenario: hll_union_agg skips null-only partial sketch states
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch, false)) AS result
        FROM (
          SELECT CAST(NULL AS BINARY) AS sketch FROM range(0, 2, 1, 1)
          UNION ALL
          SELECT hll_sketch_agg(col) AS sketch FROM VALUES (1) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 1      |

    Scenario: HLL sketch aggregates work as window functions with default arguments
      When query
        """
        WITH input AS (
          SELECT * FROM VALUES (1, 1), (2, 1), (3, 2) AS tab(id, col)
        ),
        sketches AS (
          SELECT 1 AS id, hll_sketch_agg(col) AS sketch FROM VALUES (1), (2) AS tab(col)
          UNION ALL
          SELECT 2 AS id, hll_sketch_agg(col) AS sketch FROM VALUES (2), (3) AS tab(col)
        )
        SELECT 'sketch' AS fn, id,
          hll_sketch_estimate(hll_sketch_agg(col) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS result
        FROM input
        UNION ALL
        SELECT 'union' AS fn, id,
          hll_sketch_estimate(hll_union_agg(sketch) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS result
        FROM sketches
        ORDER BY fn, id
        """
      Then query result ordered
        | fn     | id | result |
        | sketch | 1  | 1      |
        | sketch | 2  | 1      |
        | sketch | 3  | 2      |
        | union  | 1  | 2      |
        | union  | 2  | 3      |

  Rule: count_min_sketch returns Spark-compatible binary sketches

    Scenario: count_min_sketch serializes integer counts in Spark format
      When query
        """
        SELECT hex(count_min_sketch(col, 0.5d, 0.5d, 1)) AS result
        FROM VALUES (1), (2), (1) AS tab(col)
        """
      Then query result
        | result                                                                                                                       |
        | 0000000100000000000000030000000100000004000000005D8D6AB90000000000000000000000000000000200000000000000010000000000000000 |

    Scenario: count_min_sketch truncates bigint seeds like Spark
      When query
        """
        SELECT
          hex(count_min_sketch(col, 0.5d, 0.5d, CAST(2147483648 AS BIGINT))) =
          hex(count_min_sketch(col, 0.5d, 0.5d, CAST(-2147483648 AS BIGINT))) AS result
        FROM VALUES (1), (2), (1) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: count_min_sketch works as a window function
      When query
        """
        SELECT result FROM (
          SELECT hex(count_min_sketch(col, 0.5d, 0.5d, 1) OVER ()) AS result
          FROM VALUES (1), (2), (1) AS tab(col)
        )
        LIMIT 1
        """
      Then query result
        | result                                                                                                                       |
        | 0000000100000000000000030000000100000004000000005D8D6AB90000000000000000000000000000000200000000000000010000000000000000 |

  Rule: DataSketches functions return Spark-compatible types

    Scenario: HLL and count-min sketch functions return binary and bigint values
      When query
        """
        SELECT
          typeof(hll_sketch_agg(col)) AS hll_type,
          typeof(hll_sketch_estimate(hll_sketch_agg(col))) AS estimate_type,
          typeof(count_min_sketch(col, 0.5d, 0.5d, 1)) AS count_min_type
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | hll_type | estimate_type | count_min_type |
        | binary   | bigint        | binary         |

  Rule: HLL and count_min validate parameters

    Scenario: hll_sketch_agg accepts the valid lgConfigK boundaries
      When query
        """
        SELECT
          hll_sketch_estimate(hll_sketch_agg(col, 4)) AS lo,
          hll_sketch_estimate(hll_sketch_agg(col, 21)) AS hi
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | lo | hi |
        | 1  | 1  |

    Scenario: hll_sketch_agg rejects lgConfigK below the valid range
      When query
        """
        SELECT hll_sketch_agg(col, 3) FROM VALUES (1) AS tab(col)
        """
      Then query error (HLL_INVALID_LG_K|lgConfigK between 4 and 21)

    Scenario: hll_sketch_agg rejects lgConfigK above the valid range
      When query
        """
        SELECT hll_sketch_agg(col, 22) FROM VALUES (1) AS tab(col)
        """
      Then query error (HLL_INVALID_LG_K|lgConfigK between 4 and 21)

    Scenario: hll_sketch_agg rejects double input
      When query
        """
        SELECT hll_sketch_agg(CAST(col AS DOUBLE)) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|does not support input type)

    Scenario: count_min_sketch rejects non-positive eps
      When query
        """
        SELECT count_min_sketch(col, 0.0d, 0.9d, 1) FROM VALUES (1) AS tab(col)
        """
      Then query error (VALUE_OUT_OF_RANGE|eps to be positive)

    Scenario: count_min_sketch rejects confidence outside the open unit interval
      When query
        """
        SELECT count_min_sketch(col, 0.1d, 1.0d, 1) FROM VALUES (1) AS tab(col)
        """
      Then query error (VALUE_OUT_OF_RANGE|confidence to be in the range)

    Scenario: count_min_sketch rejects a null seed
      When query
        """
        SELECT count_min_sketch(col, 0.1d, 0.9d, NULL) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_NULL|integer seed argument)

    Scenario: count_min_sketch rejects decimal eps and confidence like Spark
      When query
        """
        SELECT count_min_sketch(col, 0.5, 0.5, 1) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|double eps argument)

    Scenario: count_min_sketch rejects float eps like Spark
      When query
        """
        SELECT count_min_sketch(col, CAST(0.5 AS FLOAT), 0.9d, 1) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|double eps argument)

    Scenario: count_min_sketch rejects integer confidence like Spark
      When query
        """
        SELECT count_min_sketch(col, 0.1d, 1, 1) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|double confidence argument)

    Scenario: count_min_sketch rejects decimal seed like Spark
      When query
        """
        SELECT count_min_sketch(col, 0.1d, 0.9d, CAST(1 AS DECIMAL(3,0))) FROM VALUES (1) AS tab(col)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|integer seed argument)

  Rule: HLL sketches are not byte-identical to Spark

    # The hash (MurmurHash3 seed 9001) matches Spark exactly and sketches are
    # mutually readable, but Sail's `datasketches` crate serializes the LIST/SET
    # mode compactly (vs Spark's padded form) and its dense HLL estimator diverges
    # slightly, so the serialized bytes and the estimate differ from Spark JVM.

    @sail-bug
    Scenario: hll_sketch_agg estimate matches Spark at high cardinality
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(id)) AS result
        FROM range(0, 1000)
        """
      Then query result
        | result |
        | 996    |

    @sail-bug
    Scenario: hll_sketch_agg serialized size matches Spark at low cardinality
      When query
        """
        SELECT length(hll_sketch_agg(id)) AS result
        FROM range(0, 10)
        """
      Then query result
        | result |
        | 140    |
