@kurtosis
Feature: kurtosis returns the kurtosis of the values in a group

  # `kurtosis` is a Spark SQL aggregate function, so these scenarios run against
  # both Sail and JVM Spark. Float results are rounded to keep the rendered value
  # stable across engines. Expected values validated against Spark JVM 4.1.

  Rule: kurtosis over numeric values

    Scenario: kurtosis over double values
      When query
        """
        SELECT round(kurtosis(v), 10) AS result
        FROM VALUES (1.0D), (2.0D), (3.0D), (4.0D), (100.0D) AS t(v)
        """
      Then query result
        | result       |
        | 0.2467164893 |

    Scenario: kurtosis over integer values
      When query
        """
        SELECT round(kurtosis(v), 10) AS result
        FROM VALUES (1), (2), (3), (4), (100) AS t(v)
        """
      Then query result
        | result       |
        | 0.2467164893 |

    Scenario: kurtosis skips null values
      When query
        """
        SELECT round(kurtosis(v), 10) AS result
        FROM VALUES (1.0D), (CAST(NULL AS DOUBLE)), (3.0D), (5.0D) AS t(v)
        """
      Then query result
        | result |
        | -1.5   |

    Scenario: kurtosis over numeric strings via implicit cast
      When query
        """
        SELECT round(kurtosis(v), 10) AS result
        FROM VALUES ('1'), ('2'), ('3'), ('100') AS t(v)
        """
      Then query result
        | result        |
        | -0.6674067136 |

    Scenario: kurtosis over decimal values
      When query
        """
        SELECT round(kurtosis(CAST(v AS DECIMAL(10, 2))), 10) AS result
        FROM VALUES (1), (2), (3), (4), (100) AS t(v)
        """
      Then query result
        | result       |
        | 0.2467164893 |

  Rule: kurtosis edge cases

    Scenario: kurtosis of a single value is NULL
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (5.0D) AS t(v)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: kurtosis over an empty group is NULL
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (1.0D) AS t(v) WHERE v > 100
        """
      Then query result
        | result |
        | NULL   |

  Rule: kurtosis with grouping and windows

    Scenario: kurtosis computed per group
      When query
        """
        SELECT g, round(kurtosis(v), 10) AS result
        FROM VALUES ('a', 1.0D), ('a', 2.0D), ('a', 3.0D), ('a', 4.0D), ('a', 100.0D),
                    ('b', 1.0D), ('b', 2.0D), ('b', 3.0D), ('b', 4.0D), ('b', 5.0D) AS t(g, v)
        GROUP BY g
        """
      Then query result
        | g | result       |
        | a | 0.2467164893 |
        | b | -1.3         |

    Scenario: kurtosis as a window function over a partition
      When query
        """
        SELECT g, v, round(kurtosis(v) OVER (PARTITION BY g), 10) AS result
        FROM VALUES ('a', 1.0D), ('a', 2.0D), ('a', 3.0D), ('a', 4.0D), ('a', 100.0D) AS t(g, v)
        ORDER BY v
        """
      Then query result ordered
        | g | v     | result       |
        | a | 1.0   | 0.2467164893 |
        | a | 2.0   | 0.2467164893 |
        | a | 3.0   | 0.2467164893 |
        | a | 4.0   | 0.2467164893 |
        | a | 100.0 | 0.2467164893 |

  Rule: kurtosis extreme cases

    Scenario: kurtosis of identical values (zero variance) is NULL
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (2.0D), (2.0D), (2.0D), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: kurtosis of exactly two values
      When query
        """
        SELECT round(kurtosis(v), 10) AS result FROM VALUES (1.0D), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | -2.0   |

    Scenario: kurtosis is invariant under sign negation
      When query
        """
        SELECT round(kurtosis(v), 10) AS result
        FROM VALUES (-1.0D), (-2.0D), (-3.0D), (-4.0D), (-100.0D) AS t(v)
        """
      Then query result
        | result       |
        | 0.2467164893 |

  Rule: kurtosis input type validation

    # Spark rejects boolean input with DATATYPE_MISMATCH; Sail rejects it from
    # `coerce_types` with an unsupported-type error. The regex matches both.
    # BOOLEAN is not implicitly castable to DOUBLE, so the rejection is an
    # analysis-time type check that is NOT gated by spark.sql.ansi.enabled:
    # Spark rejects in BOTH modes (validated against Spark JVM 4.2), and Sail
    # matches by rejecting unconditionally. The pair guards against anyone making
    # the coercion ANSI-conditional.
    Scenario: kurtosis rejects boolean input in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (true), (false), (true) AS t(v)
        """
      Then query error (DATATYPE_MISMATCH|numeric type)

    Scenario: kurtosis rejects boolean input in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (true), (false), (true) AS t(v)
        """
      Then query error (DATATYPE_MISMATCH|numeric type)

  Rule: kurtosis with special float values

    # Any NaN in the group propagates to a NaN result: m2 is NaN (not zero), so
    # this is not the divide-by-zero NULL branch. Validated against Spark JVM 4.2.
    Scenario: kurtosis with a NaN value is NaN
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (1.0D), (2.0D), (double('nan')) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    # An infinite value makes the central moments non-finite; Inf/Inf yields NaN.
    Scenario: kurtosis with an infinite value is NaN
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (1.0D), (2.0D), (double('inf')) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    # Values are individually finite but the higher-order moments overflow double
    # to Infinity, so the result is NaN rather than a finite number.
    Scenario: kurtosis overflows to NaN for very large magnitudes
      When query
        """
        SELECT kurtosis(v) AS result
        FROM VALUES (1.0E150D), (2.0E150D), (3.0E150D), (4.0E150D) AS t(v)
        """
      Then query result
        | result |
        | NaN    |

    # -0.0 and 0.0 compare equal, so the group has zero variance (m2 = 0) and
    # kurtosis is NULL — not NaN — under the default (non-legacy) config.
    Scenario: kurtosis of negative and positive zero is NULL
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (CAST('-0.0' AS DOUBLE)), (0.0D), (0.0D) AS t(v)
        """
      Then query result
        | result |
        | NULL   |

  Rule: kurtosis over float inputs and with modifiers

    Scenario: kurtosis over float (32-bit) values
      When query
        """
        SELECT round(kurtosis(CAST(v AS FLOAT)), 10) AS result
        FROM VALUES (1), (2), (3), (100) AS t(v)
        """
      Then query result
        | result        |
        | -0.6674067136 |

    Scenario: kurtosis with DISTINCT deduplicates before aggregating
      When query
        """
        SELECT round(kurtosis(DISTINCT v), 10) AS result
        FROM VALUES (1.0D), (1.0D), (2.0D), (3.0D), (100.0D) AS t(v)
        """
      Then query result
        | result        |
        | -0.6674067136 |

    Scenario: kurtosis with a FILTER clause aggregates only matching rows
      When query
        """
        SELECT round(kurtosis(v) FILTER (WHERE v < 50), 10) AS result
        FROM VALUES (1.0D), (2.0D), (3.0D), (100.0D) AS t(v)
        """
      Then query result
        | result |
        | -1.5   |

  Rule: kurtosis under legacy statistical aggregate mode

    # Under spark.sql.legacy.statisticalAggregate=true, the divide-by-zero branch
    # (m2 == 0: all values identical, or a single value) returns NaN instead of
    # NULL (`divideByZeroEvalResult = if nullOnDivideByZero NULL else NaN`). Sail
    # registers the config but ignores its effect and always returns NULL, so this
    # is a real divergence — validated against Spark JVM 4.2.
    @sail-bug
    Scenario: kurtosis of identical values is NaN in legacy mode
      Given config spark.sql.legacy.statisticalAggregate = true
      When query
        """
        SELECT kurtosis(v) AS result FROM VALUES (2.0D), (2.0D), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | NaN    |
