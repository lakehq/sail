Feature: DataFrame sample operations

  Rule: Sample without replacement

    Scenario: sample with fraction returns subset
      When query
      """
      SELECT COUNT(*) AS cnt FROM (
        SELECT id FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10)
      ) t
      """
      Then query result
      | cnt |
      | 10  |

  Rule: Sample with seed produces deterministic results

    Scenario: same seed produces same sample
      When query
      """
      SELECT id FROM (
        SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
      ) ORDER BY id
      """
      Then query result
      | id |
      | 1  |
      | 2  |
      | 3  |
      | 4  |
      | 5  |

  Rule: Random function with seed

    Scenario: rand with same seed returns same value
      When query
      """
      SELECT CAST(rand(1) * 1000000 AS INT) AS r
      """
      Then query result
      | r      |
      | 636378 |

    Scenario: rand with different seed returns different value
      When query
      """
      SELECT CAST(rand(24) * 1000000 AS INT) AS r
      """
      Then query result
      | r      |
      | 394325 |
