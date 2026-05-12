@ceil_floor_simplify
Feature: ceil() / floor() — simplify hook (integer identity + idempotence)

  # SparkCeil and SparkFloor implement `fn simplify`:
  #   • Integer identity: ceil/floor of an integer column is the integer itself
  #     (the 1-arg return type is always BIGINT, so narrower ints get a cast).
  #   • Idempotence: ceil(ceil(x)) = ceil(x), floor(floor(x)) = floor(x).
  # Both rules fire during DataFusion's SimplifyExpressions logical pass,
  # removing UDF call overhead for common patterns.

  Rule: Integer identity — ceil/floor of an integer input is a no-op

    Scenario: ceil of INT keeps the value
      When query
        """
        SELECT ceil(v) AS c FROM VALUES
          (CAST(3 AS INT)),
          (CAST(-5 AS INT)),
          (CAST(0 AS INT))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c  |
        | -5 |
        | 0  |
        | 3  |

    Scenario: floor of INT keeps the value
      When query
        """
        SELECT floor(v) AS c FROM VALUES
          (CAST(3 AS INT)),
          (CAST(-5 AS INT)),
          (CAST(0 AS INT))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c  |
        | -5 |
        | 0  |
        | 3  |

    Scenario: ceil of BIGINT keeps the value including boundaries
      When query
        """
        SELECT ceil(v) AS c FROM VALUES
          (CAST(9223372036854775807 AS BIGINT)),
          (CAST(-9223372036854775808 AS BIGINT))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c                    |
        | -9223372036854775808 |
        | 9223372036854775807  |

    Scenario: NULL input returns NULL for integer identity path
      When query
        """
        SELECT ceil(CAST(NULL AS INT)) AS a, floor(CAST(NULL AS INT)) AS b
        """
      Then query result
        | a    | b    |
        | NULL | NULL |

  Rule: Idempotence — ceil(ceil(x)) = ceil(x), floor(floor(x)) = floor(x)

    Scenario: ceil of ceil returns same result as single ceil
      When query
        """
        SELECT ceil(ceil(v)) AS c FROM VALUES
          (CAST(1.7 AS DOUBLE)),
          (CAST(-1.7 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c  |
        | -1 |
        | 2  |
        | 2  |

    Scenario: floor of floor returns same result as single floor
      When query
        """
        SELECT floor(floor(v)) AS c FROM VALUES
          (CAST(1.7 AS DOUBLE)),
          (CAST(-1.7 AS DOUBLE)),
          (CAST(2.0 AS DOUBLE))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c  |
        | -2 |
        | 1  |
        | 2  |

  Rule: 2-arg form is NOT simplified (genuine rounding)

    Scenario: ceil with negative scale still rounds up
      When query
        """
        SELECT ceil(v, -1) AS c FROM VALUES
          (CAST(1 AS INT)),
          (CAST(15 AS INT)),
          (CAST(100 AS INT))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c   |
        | 10  |
        | 20  |
        | 100 |

    Scenario: floor with negative scale still rounds down
      When query
        """
        SELECT floor(v, -1) AS c FROM VALUES
          (CAST(1 AS INT)),
          (CAST(15 AS INT)),
          (CAST(100 AS INT))
        AS t(v) ORDER BY c
        """
      Then query result ordered
        | c   |
        | 0   |
        | 10  |
        | 100 |

  Rule: Plan snapshot — simplify removes UDF call for integer input

    @sail-only
    Scenario: EXPLAIN ceil of INT — no spark_ceil in plan
      When query
        """
        EXPLAIN SELECT ceil(v) AS c FROM VALUES
          (CAST(1 AS INT)),
          (CAST(2 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN ceil of BIGINT — no spark_ceil in plan, no cast needed
      When query
        """
        EXPLAIN SELECT ceil(v) AS c FROM VALUES
          (CAST(1 AS BIGINT)),
          (CAST(2 AS BIGINT)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN floor of INT — no spark_floor in plan
      When query
        """
        EXPLAIN SELECT floor(v) AS c FROM VALUES
          (CAST(1 AS INT)),
          (CAST(2 AS INT)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN ceil of DOUBLE — spark_ceil stays in plan
      When query
        """
        EXPLAIN SELECT ceil(v) AS c FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN ceil of ceil — idempotence removes outer call
      When query
        """
        EXPLAIN SELECT ceil(ceil(v)) AS c FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN floor of floor — idempotence removes outer call
      When query
        """
        EXPLAIN SELECT floor(floor(v)) AS c FROM VALUES
          (CAST(1.5 AS DOUBLE)),
          (CAST(2.5 AS DOUBLE)) AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN ceil with negative scale — spark_ceil stays in plan
      When query
        """
        EXPLAIN SELECT ceil(v, -1) AS c FROM VALUES
          (CAST(15 AS INT)) AS t(v)
        """
      Then query plan matches snapshot
