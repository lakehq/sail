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

  Rule: Sample without replacement bound validation

    Scenario Outline: sample without replacement rejects invalid individual bounds
      When dataframe sample without replacement with bounds <lower_bound> and <upper_bound>
      Then dataframe error <error>

      Examples:
        | lower_bound | upper_bound | error                                    |
        | 5e-324      | -0.000001   | Lower bound .* must be <= upper bound .* |
        | -0.000002   | -0.000002   | Lower bound .* must be >= 0[.]0          |
        | 1.000002    | 1.000002    | Upper bound .* must be <= 1[.]0          |

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

  Rule: Internal Poisson sampler argument validation

    @sail-only
    Scenario: zero lambda still validates a non-scalar seed
      When query
        """
        SELECT random_poisson(CAST(0 AS DOUBLE), id) AS result FROM range(3)
        """
      Then query error (?i)random_poisson.*scalar seed
