Feature: randn function (Gaussian/normal distribution)

  # IMPLEMENTATION NOTE:
  # Sail uses `SparkXorShiftRandom::next_gaussian` (port of Spark's
  # `XORShiftRandom`/`Random.nextGaussian`). For any given seed the sequence
  # matches Spark JVM bit-for-bit.

  Rule: Arity validation

    Scenario: randn with two args fails
      When query
        """
        SELECT randn(1, 2) AS r
        """
      Then query error (?i).*

  Rule: Seed type validation

    Scenario: randn rejects string seed
      When query
        """
        SELECT randn('foo') AS r
        """
      Then query error (?i).*

    Scenario: randn rejects decimal seed
      When query
        """
        SELECT randn(3.14) AS r
        """
      Then query error (?i).*

    Scenario: randn rejects tinyint seed
      When query
        """
        SELECT randn(CAST(42 AS TINYINT)) AS r
        """
      Then query error (?i).*

    Scenario: randn rejects smallint seed
      When query
        """
        SELECT randn(CAST(42 AS SMALLINT)) AS r
        """
      Then query error (?i).*

    Scenario: randn rejects non-foldable seed from column
      When query
        """
        SELECT randn(CAST(id AS INT)) AS r FROM range(3)
        """
      Then query error (?i).*

  Rule: Bit-exact values match Spark JVM

    Scenario: randn with seed 0 matches Spark
      When query
        """
        SELECT randn(0) AS r
        """
      Then query result
        | r                  |
        | 1.6034991609278433 |

    Scenario: randn with seed 1 matches Spark
      When query
        """
        SELECT randn(1) AS r
        """
      Then query result
        | r                  |
        | 1.6845611254444919 |

    Scenario: randn with seed 24 matches Spark
      When query
        """
        SELECT randn(24) AS r
        """
      Then query result
        | r                   |
        | -2.4656575828961267 |

    Scenario: randn with seed 42 matches Spark
      When query
        """
        SELECT randn(42) AS r
        """
      Then query result
        | r                 |
        | 2.384479054241165 |

    Scenario: randn with negative seed matches Spark
      When query
        """
        SELECT randn(-1) AS r
        """
      Then query result
        | r                   |
        | -1.1760378827493814 |

    Scenario: randn with INT seed behaves as BIGINT
      When query
        """
        SELECT randn(CAST(42 AS INT)) AS r
        """
      Then query result
        | r                 |
        | 2.384479054241165 |

    Scenario: randn with seed 42 multi-row matches Spark
      When query
        """
        SELECT randn(42) AS r FROM range(5)
        """
      Then query result
        | r                   |
        | 2.384479054241165   |
        | 0.1920934041293524  |
        | 0.7337336533286575  |
        | -0.5224480195716871 |
        | 2.060084179317831   |

    Scenario: randn with seed 0 multi-row matches Spark
      When query
        """
        SELECT randn(0) AS r FROM range(5)
        """
      Then query result
        | r                    |
        | 1.6034991609278433   |
        | 0.14416006165776865  |
        | -0.6253564498627744  |
        | -0.28385414448030416 |
        | 0.9333495004884642   |

  Rule: NULL seed is equivalent to seed 0

    # Spark treats a literal NULL seed as a concrete seed of 0 (not as "no
    # seed"), so `randn(NULL)` is deterministic and matches `randn(0)`.

    Scenario: randn with NULL seed equals randn with seed 0
      When query
        """
        SELECT randn(NULL) AS r
        """
      Then query result
        | r                  |
        | 1.6034991609278433 |

  Rule: No-seed returns a value

    Scenario: randn without seed returns a value
      When query
        """
        SELECT randn() IS NOT NULL AS has_value
        """
      Then query result
        | has_value |
        | true      |

  Rule: Seed near Long.MAX_VALUE wraps correctly

    # seed + partitionIndex must use wrapping (Java long) arithmetic.
    # Without wrapping_add, debug builds panic on overflow when partitionIndex > 0.

    Scenario: randn with Long.MAX_VALUE seed matches Spark
      When query
        """
        SELECT randn(9223372036854775807) AS r
        """
      Then query result
        | r                        |
        | -0.023841785718521923    |

  Rule: Seeded randn is supported in WHERE clause

    Scenario: randn with seed filters rows deterministically in WHERE
      When query
        """
        SELECT id FROM range(10) WHERE randn(0) > 0
        """
      Then query result
        | id |
        | 0  |
        | 1  |
        | 4  |
        | 7  |
        | 9  |

  Rule: Empty batch

    Scenario: randn on empty batch returns empty result
      When query
        """
        SELECT randn(0) AS r FROM range(0)
        """
      Then query result
        | r |
