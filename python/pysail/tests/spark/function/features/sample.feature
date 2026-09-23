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

    @sail-bug
    Scenario: a seeded sample uses Spark's partition-specific seed
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW sample_data AS
        SELECT * FROM range(0, 10, 1, 2)
        """
      When query
        """
        SELECT CAST(array_sort(collect_list(id)) AS STRING) AS sampled_ids
        FROM sample_data TABLESAMPLE (50 PERCENT) REPEATABLE (1)
        """
      Then query result
        | sampled_ids     |
        | [2, 3, 6, 7, 8] |

    @sail-bug
    Scenario: a seeded sample advances its RNG across execution batches
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW sample_data AS
        SELECT * FROM range(0, 2048, 1, 1)
        """
      When query
        """
        SELECT count_if(id < 1024) AS first_batch,
               count_if(id >= 1024) AS second_batch
        FROM sample_data TABLESAMPLE (50 PERCENT) REPEATABLE (42)
        """
      Then query result
        | first_batch | second_batch |
        | 527         | 531          |

  Rule: Sample with replacement

    @sail-bug
    Scenario: a seeded replacement sample matches Spark's Poisson sampler
      Given sample with replacement fraction 0.5 seed 1 as temporary view sample_data
      When query
        """
        SELECT CAST(array_sort(collect_list(id)) AS STRING) AS sampled_ids
        FROM sample_data
        """
      Then query result
        | sampled_ids        |
        | [0, 2, 3, 4, 7, 9] |

    @sail-bug
    Scenario: a zero replacement sample skips a child projection
      When dataframe replacement sample fraction 0.0 over failing projection
      Then dataframe is empty

    Scenario: a zero replacement sample still evaluates a child filter
      When dataframe replacement sample fraction 0.0 over failing filter
      Then dataframe error filter-error

  Rule: Sample without replacement bound validation

    @sail-bug
    Scenario: an invalid fraction is checked before child analysis
      When dataframe sample fraction 2.0 over unresolved projection
      Then dataframe error Sampling fraction .* must be on interval

    Scenario: a child construction error precedes fraction validation
      When dataframe sample fraction 2.0 over zero-step range
      Then dataframe error (?i)(step .* cannot be 0|range step must not be 0)

    Scenario Outline: sample without replacement rejects invalid individual bounds
      When dataframe sample without replacement with bounds <lower_bound> and <upper_bound>
      Then dataframe error <error>

      Examples:
        | lower_bound | upper_bound | error                                    |
        | 5e-324      | -0.000001   | Lower bound .* must be <= upper bound .* |
        | -0.000002   | -0.000002   | Lower bound .* must be >= 0[.]0          |
        | 1.000002    | 1.000002    | Upper bound .* must be <= 1[.]0          |

    @sail-bug
    Scenario: invalid individual sample bounds are deferred until execution
      When dataframe sample without replacement with bounds -0.000002 and -0.000001
      Then dataframe schema
        """
        root
         |-- id: long (nullable = false)
        """

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

    Scenario: sample without replacement fraction 0.3 seed 42
      When query
        """
        SELECT id FROM (
          SELECT id, rand(42) AS r FROM range(10)
        ) t WHERE r < 0.3 ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 7  |

  Rule: Internal Poisson sampler argument validation

    @sail-only
    Scenario: zero lambda still validates a non-scalar seed
      When query
        """
        SELECT random_poisson(CAST(0 AS DOUBLE), id) AS result FROM range(3)
        """
      Then query error (?i)random_poisson.*scalar seed
