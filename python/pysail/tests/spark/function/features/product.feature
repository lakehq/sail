@product @sail-only
Feature: product returns the multiplicative product of all non-null input values

  # NOTE: Spark SQL does NOT currently recognise `product` as a routine —
  # `SELECT product(...)` raises `UNRESOLVED_ROUTINE` on JVM Spark; the function
  # is exposed ONLY through the DataFrame API (`pyspark.sql.functions.product`).
  # Sail, however, registers `product` as a SQL aggregate and window function,
  # so this whole feature is tagged `@sail-only` (skipped on JVM Spark).
  #
  # The DataFrame-API equivalent of these scenarios lives in the sibling file
  # `python/pysail/tests/spark/function/test_product.py`, which runs against both
  # Sail and JVM Spark.
  #
  # When Spark SQL eventually enables `product` as a routine, remove the
  # `@sail-only` tag from this feature AND delete `test_product.py`.
  #
  # Expected values validated against Spark JVM 4.1.

  Rule: product multiplies non-null values

    Scenario: product over a basic group-by from the PySpark doctest
      When query
        """
        SELECT mod3, product(value) AS p
        FROM (SELECT id % 3 AS mod3, id AS value FROM RANGE(10))
        GROUP BY mod3
        ORDER BY mod3
        """
      Then query result ordered
        | mod3 | p    |
        | 0    | 0.0  |
        | 1    | 28.0 |
        | 2    | 80.0 |

    Scenario: product over integer values returns a double
      When query
        """
        SELECT typeof(p) AS t FROM (
          SELECT product(col) AS p
          FROM VALUES (1), (2), (3) AS tab(col)
        )
        """
      Then query result
        | t      |
        | double |

    Scenario: product over float values
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (1.5), (2.0), (4.0) AS tab(col)
        """
      Then query result
        | p    |
        | 12.0 |

    Scenario: product works as a window function
      When query
        """
        SELECT id, product(value) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS p
        FROM VALUES (1, 2), (2, 3), (3, 4) AS tab(id, value)
        """
      Then query result ordered
        | id | p    |
        | 1  | 2.0  |
        | 2  | 6.0  |
        | 3  | 24.0 |

  Rule: product handles null inputs

    Scenario: product skips null values
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (2), (CAST(NULL AS INT)), (3), (4) AS tab(col)
        """
      Then query result
        | p    |
        | 24.0 |

    Scenario: product over all-null group returns null
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col)
        """
      Then query result
        | p    |
        | NULL |

    Scenario: product over an empty group returns null
      When query
        """
        SELECT product(col) AS p
        FROM (SELECT 1 AS col WHERE false) AS tab
        """
      Then query result
        | p    |
        | NULL |

  Rule: product supports group-by with null groups

    Scenario: product groups including a null-only group
      When query
        """
        SELECT g, product(v) AS p
        FROM VALUES
          ('a', 2),
          ('a', 5),
          ('b', CAST(NULL AS INT)),
          ('b', CAST(NULL AS INT)),
          ('c', 3),
          ('c', CAST(NULL AS INT)),
          ('c', 4)
          AS tab(g, v)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | p    |
        | a | 10.0 |
        | b | NULL |
        | c | 12.0 |

  Rule: Edge cases validated against Spark JVM

    Scenario: product returns a single value unchanged as a double
      When query
        """
        SELECT product(v) AS result FROM VALUES (5) AS t(v)
        """
      Then query result
        | result |
        | 5.0    |

    Scenario: product returns zero when any value is zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (2), (0), (3) AS t(v)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: product multiplies negative values with the correct sign
      When query
        """
        SELECT product(v) AS result FROM VALUES (-2), (3), (-4) AS t(v)
        """
      Then query result
        | result |
        | 24.0   |

    Scenario: product overflows to Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (1e200D), (1e200D) AS t(v)
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: product propagates NaN
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('nan')), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    Scenario: product returns NaN for Infinity times zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('inf')), (0.0D) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    # @sail-bug: Sail renders large doubles in scientific notation as `1e18`,
    # while Spark renders `1.0E18` (missing `.0` mantissa, lowercase `e`).
    # This is a general float-to-string formatting gap, not specific to product.
    @sail-bug
    Scenario: product accepts bigint input and returns a double
      When query
        """
        SELECT product(CAST(v AS BIGINT)) AS result FROM VALUES (1000000), (1000000), (1000000) AS t(v)
        """
      Then query result
        | result |
        | 1.0E18 |

    Scenario: product accepts decimal input
      When query
        """
        SELECT product(CAST(v AS DECIMAL(10, 2))) AS result FROM VALUES (1), (2), (3) AS t(v)
        """
      Then query result
        | result |
        | 6.0    |

    Scenario: product accepts numeric strings via implicit cast
      When query
        """
        SELECT product(v) AS result FROM VALUES ('2'), ('3') AS t(v)
        """
      Then query result
        | result |
        | 6.0    |

  Rule: Infinity propagation and sign

    Scenario: product keeps a lone positive Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('inf')) AS t(v)
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: product keeps a lone negative Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('-inf')) AS t(v)
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: product multiplies positive and negative Infinity to negative Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('inf')), (double('-inf')) AS t(v)
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: product multiplies two negative Infinities to positive Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('-inf')), (double('-inf')) AS t(v)
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: product multiplies Infinity by a negative number to negative Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('inf')), (-2.0D) AS t(v)
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: product returns NaN for negative Infinity times zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('-inf')), (0.0D) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    Scenario: product keeps Infinity when NULL values are skipped
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('inf')), (CAST(NULL AS DOUBLE)) AS t(v)
        """
      Then query result
        | result   |
        | Infinity |

  Rule: NaN propagation

    Scenario: product keeps a lone NaN
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('nan')) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    Scenario: product returns NaN for NaN times Infinity
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('nan')), (double('inf')) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    Scenario: product keeps NaN when NULL values are skipped
      When query
        """
        SELECT product(v) AS result FROM VALUES (double('nan')), (CAST(NULL AS DOUBLE)) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

  Rule: Signed zero, underflow and identity

    Scenario: product preserves negative zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (CAST('-0.0' AS DOUBLE)) AS t(v)
        """
      Then query result
        | result |
        | -0.0   |

    Scenario: product produces negative zero from a negative value times zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (-2.0D), (0.0D) AS t(v)
        """
      Then query result
        | result |
        | -0.0   |

    Scenario: product underflows to zero
      When query
        """
        SELECT product(v) AS result FROM VALUES (1e-300D), (1e-300D) AS t(v)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: product multiplies a tiny and a huge value back to one
      When query
        """
        SELECT product(v) AS result FROM VALUES (1e-200D), (1e200D) AS t(v)
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: product multiplies ones to one
      When query
        """
        SELECT product(v) AS result FROM VALUES (1.0D), (1.0D), (1.0D) AS t(v)
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: product multiplies fractions
      When query
        """
        SELECT product(v) AS result FROM VALUES (0.5D), (0.5D), (0.5D) AS t(v)
        """
      Then query result
        | result |
        | 0.125  |

    Scenario: product returns the lone non-null value when a NULL precedes it
      When query
        """
        SELECT product(v) AS result FROM VALUES (CAST(NULL AS INT)), (7) AS t(v)
        """
      Then query result
        | result |
        | 7.0    |

  Rule: Input type validation

    # Spark rejects boolean input with DATATYPE_MISMATCH (product requires DOUBLE);
    # Sail rejects it from `coerce_types` with an unsupported-type error.
    Scenario: product rejects boolean input
      When query
        """
        SELECT product(v) AS result FROM VALUES (true), (false) AS t(v)
        """
      Then query error expects a numeric type
