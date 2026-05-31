Feature: rand function (uniform [0, 1) distribution)

  # IMPLEMENTATION NOTE:
  # Sail uses `SparkXorShiftRandom::next_double` (port of Spark's
  # `XORShiftRandom`/`Random.nextDouble`). For any given seed the sequence
  # matches Spark JVM bit-for-bit.

  Rule: Arity validation

    Scenario: rand with two args fails
      When query
        """
        SELECT rand(1, 2) AS r
        """
      Then query error (?i).*

  Rule: Seed type validation

    Scenario: rand rejects string seed
      When query
        """
        SELECT rand('foo') AS r
        """
      Then query error (?i).*

    Scenario: rand rejects decimal seed
      When query
        """
        SELECT rand(3.14) AS r
        """
      Then query error (?i).*

    Scenario: rand rejects tinyint seed
      When query
        """
        SELECT rand(CAST(42 AS TINYINT)) AS r
        """
      Then query error (?i).*

    Scenario: rand rejects smallint seed
      When query
        """
        SELECT rand(CAST(42 AS SMALLINT)) AS r
        """
      Then query error (?i).*

    Scenario: rand rejects non-foldable seed from column
      When query
        """
        SELECT rand(CAST(id AS INT)) AS r FROM range(3)
        """
      Then query error (?i).*

  Rule: Bit-exact values match Spark JVM

    Scenario: rand with seed 0 matches Spark
      When query
        """
        SELECT rand(0) AS r
        """
      Then query result
        | r                  |
        | 0.7604953758285915 |

    Scenario: rand with seed 1 matches Spark
      When query
        """
        SELECT rand(1) AS r
        """
      Then query result
        | r                  |
        | 0.6363787615254752 |

    Scenario: rand with seed 24 matches Spark
      When query
        """
        SELECT rand(24) AS r
        """
      Then query result
        | r                  |
        | 0.3943255396952755 |

    Scenario: rand with seed 42 matches Spark
      When query
        """
        SELECT rand(42) AS r
        """
      Then query result
        | r                 |
        | 0.619189370225301 |

    Scenario: rand with negative seed matches Spark
      When query
        """
        SELECT rand(-1) AS r
        """
      Then query result
        | r                  |
        | 0.3030262136082562 |

    Scenario: rand with INT seed behaves as BIGINT
      When query
        """
        SELECT rand(CAST(42 AS INT)) AS r
        """
      Then query result
        | r                 |
        | 0.619189370225301 |

    Scenario: rand with seed 0 multi-row matches Spark
      When query
        """
        SELECT rand(0) AS r FROM range(5)
        """
      Then query result
        | r                  |
        | 0.7604953758285915 |
        | 0.5234194256885571 |
        | 0.0953472826424725 |
        | 0.3163249920547614 |
        | 0.7141011170991605 |

    Scenario: rand with seed 42 multi-row matches Spark
      When query
        """
        SELECT rand(42) AS r FROM range(5)
        """
      Then query result
        | r                   |
        | 0.619189370225301   |
        | 0.5096018842446481  |
        | 0.8325259388871524  |
        | 0.26322809041172357 |
        | 0.6702867696264135  |

  Rule: NULL seed is equivalent to seed 0

    # Spark treats a literal NULL seed as a concrete seed of 0 (not as "no
    # seed"), so `rand(NULL)` is deterministic and matches `rand(0)`.

    Scenario: rand with NULL seed equals rand with seed 0
      When query
        """
        SELECT rand(NULL) AS r
        """
      Then query result
        | r                  |
        | 0.7604953758285915 |

  Rule: No-seed returns a value in range

    Scenario: rand without seed returns a value in [0, 1)
      When query
        """
        SELECT rand() >= 0 AND rand() < 1 AS in_range
        """
      Then query result
        | in_range |
        | true     |

  Rule: Seed near Long.MAX_VALUE wraps correctly

    # seed + partitionIndex must use wrapping (Java long) arithmetic.
    # Without wrapping_add, debug builds panic on overflow when partitionIndex > 0.

    Scenario: rand with Long.MAX_VALUE seed matches Spark
      When query
        """
        SELECT rand(9223372036854775807) AS r
        """
      Then query result
        | r                      |
        | 0.17715881442674208    |

    Scenario: rand with Long.MAX_VALUE seed multi-row matches Spark
      When query
        """
        SELECT rand(9223372036854775807) AS r FROM range(5)
        """
      Then query result
        | r                      |
        | 0.17715881442674208    |
        | 0.9077141821545979     |
        | 0.08354279996053304    |
        | 0.12007064558690794    |
        | 0.4640324942790296     |

  Rule: Seeded rand is supported in WHERE clause

    Scenario: rand with seed filters rows deterministically in WHERE
      When query
        """
        SELECT id FROM range(10) WHERE rand(42) > 0.5
        """
      Then query result
        | id |
        | 0  |
        | 1  |
        | 2  |
        | 4  |
        | 5  |
        | 6  |
        | 8  |
        | 9  |

  Rule: Seeded rand is supported in HAVING clause

    Scenario: rand with seed filters groups deterministically in HAVING
      When query
        """
        SELECT id % 3 AS grp, count(*) AS cnt FROM range(10) GROUP BY id % 3 HAVING rand(42) > 0.5
        """
      Then query result
        | grp | cnt |
        | 0   | 4   |
        | 1   | 3   |
        | 2   | 3   |

  Rule: Empty batch

    Scenario: rand on empty batch returns empty result
      When query
        """
        SELECT rand(0) AS r FROM range(0)
        """
      Then query result
        | r |
