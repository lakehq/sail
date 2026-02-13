Feature: DataFrame sample operations

  Rule: Sample without replacement

    Scenario: filtering with rand and seed returns expected rows
      When query
        """
        SELECT id FROM (
          SELECT id, rand(1) AS r FROM range(10)
        ) t WHERE r < 0.5 ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
        | 3  |
        | 6  |
        | 7  |
        | 8  |

  Rule: Random function with seed

    Scenario: rand with seed 1 returns deterministic value
      When query
        """
        SELECT CAST(rand(1) * 1000000 AS INT) AS r
        """
      Then query result
        | r      |
        | 636378 |

    Scenario: rand with seed 24 returns deterministic value
      When query
        """
        SELECT CAST(rand(24) * 1000000 AS INT) AS r
        """
      Then query result
        | r      |
        | 394325 |

    Scenario: rand without seed returns a value
      When query
        """
        SELECT rand() IS NOT NULL AS has_value
        """
      Then query result
        | has_value |
        | true      |

    Scenario: rand with different seeds in same query
      When query
        """
        SELECT
          CAST(rand(1) * 1000000 AS INT) AS r1,
          CAST(rand(24) * 1000000 AS INT) AS r24
        """
      Then query result
        | r1     | r24    |
        | 636378 | 394325 |

    Scenario: rand with seed over range produces deterministic sequence
      When query
        """
        SELECT id, CAST(rand(1) * 10000 AS BIGINT) AS r
        FROM range(0, 5)
        ORDER BY id
        """
      Then query result ordered
        | id | r    |
        | 0  | 6363 |
        | 1  | 5993 |
        | 2  | 1348 |
        | 3  | 768  |
        | 4  | 8539 |
