Feature: randn function (Gaussian/normal distribution)

  Rule: randn with seed produces deterministic Gaussian values

    Scenario: randn with seed 42 returns expected value
      When query
      """
      SELECT CAST(randn(42) * 1000000 AS BIGINT) AS r
      """
      Then query result
      | r       |
      | 2384479 |

    Scenario: randn with seed 1 returns expected value
      When query
      """
      SELECT CAST(randn(1) * 1000000 AS BIGINT) AS r
      """
      Then query result
      | r       |
      | 1684561 |

    Scenario: randn with seed 24 returns expected value
      When query
      """
      SELECT CAST(randn(24) * 1000000 AS BIGINT) AS r
      """
      Then query result
      | r        |
      | -2465657 |

  Rule: randn with different seeds returns different values

    Scenario: randn values differ across seeds
      When query
      """
      SELECT
        CAST(randn(1) * 1000000 AS BIGINT) AS r1,
        CAST(randn(42) * 1000000 AS BIGINT) AS r42
      """
      Then query result
      | r1      | r42     |
      | 1684561 | 2384479 |

  Rule: randn without seed is non-deterministic

    Scenario: randn without seed returns a value
      When query
      """
      SELECT randn() IS NOT NULL AS has_value
      """
      Then query result
      | has_value |
      | true      |

  Rule: randn produces multiple rows with range

    Scenario: randn with seed over range produces expected sequence
      When query
      """
      SELECT id, CAST(randn(42) * 10000 AS BIGINT) AS r
      FROM range(0, 5)
      ORDER BY id
      """
      Then query result ordered
      | id | r      |
      | 0  | 23844  |
      | 1  | 1920   |
      | 2  | 7337   |
      | 3  | -5224  |
      | 4  | 20600  |
