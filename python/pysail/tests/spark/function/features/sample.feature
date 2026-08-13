Feature: DataFrame sample operations

  Rule: Sample without replacement

    Scenario Outline: a fractional sample matches Spark for seed <seed>
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW sample_data AS
        SELECT * FROM range(0, 10, 1, 1)
        """
      When query
        """
        SELECT CAST(array_sort(collect_list(id)) AS STRING) AS sampled_ids
        FROM sample_data TABLESAMPLE (50 PERCENT) REPEATABLE (<seed>)
        """
      Then query result
        | sampled_ids   |
        | <sampled_ids> |

      Examples:
        | seed | sampled_ids               |
        | 1    | [2, 3, 6, 7, 8]           |
        | 2    | [1, 2, 3, 5, 6, 7, 9]     |

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
