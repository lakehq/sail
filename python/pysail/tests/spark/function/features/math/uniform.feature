Feature: uniform() generates random numbers within a range

  # IMPLEMENTATION NOTE:
  # Sail uses Rust's StdRng (ChaCha20-based RNG) while Spark uses Java's Random (LCG).
  # Generated values differ between implementations but both are statistically uniform
  # and reproducible with the same seed in their respective environments.

  Rule: Basic random number generation with seed

    Scenario: uniform generates integer in range with seed 0
      When query
        """
        SELECT uniform(10, 20, 0) AS result
        """
      Then query result ordered
        | result |
        | 18     |

    Scenario: uniform generates integer in larger range with seed 42
      When query
        """
        SELECT uniform(0, 100, 42) AS result
        """
      Then query result ordered
        | result |
        | 13     |

    Scenario: uniform generates decimal in range with seed 123
      When query
        """
        SELECT uniform(5.5, 10.5, 123) AS result
        """
      Then query result ordered
        | result |
        | 6.2    |

    Scenario: uniform handles negative seed
      When query
        """
        SELECT uniform(5, 105, -3) AS result
        """
      Then query result ordered
        | result |
        | 17     |

  Rule: Schema type inference for integers

    Scenario: uniform returns integer type for integer inputs
      When query
        """
        SELECT uniform(10, 20) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: uniform returns integer type for INT_MAX bounds
      When query
        """
        SELECT uniform(2147483647, 2147483647, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: uniform returns long type when exceeding INT_MAX
      When query
        """
        SELECT uniform(2147483647, 21474836471, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: uniform returns long type for bigint range
      When query
        """
        SELECT uniform(
          CAST(2147483648 AS BIGINT),
          CAST(9223372036854775807 AS BIGINT),
          42
        ) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

  Rule: Schema type inference for decimals

    Scenario: uniform returns decimal type for decimal inputs
      When query
        """
        SELECT uniform(5.5, 10.5, 123) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(3,1) (nullable = false)
        """

    Scenario: uniform returns decimal type for mixed decimal and integer
      When query
        """
        SELECT uniform(5.5, 10, 123) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(2,1) (nullable = false)
        """

    Scenario: uniform uses larger decimal precision
      When query
        """
        SELECT uniform(1, 12345.67890, 42) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(10,5) (nullable = false)
        """

    Scenario: uniform uses decimal scale from input
      When query
        """
        SELECT uniform(1, 12.34567890, 42) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(10,8) (nullable = false)
        """

    Scenario: uniform with large decimal precision
      When query
        """
        SELECT uniform(
          1.2,
          12345678901234567890,
          42
        ) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(20,0) (nullable = false)
        """

    Scenario: uniform decimal ignores integer type when decimal present
      When query
        """
        SELECT uniform(1.2, 2147483647, 42) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(2,1) (nullable = false)
        """

    Scenario: uniform decimal ignores bigint type when decimal present
      When query
        """
        SELECT uniform(
          1.2,
          CAST(9223372036854775807 AS BIGINT),
          43
        ) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(2,1) (nullable = false)
        """

  Rule: Schema type inference for small integer types

    Scenario: uniform returns byte type for tinyint inputs
      When query
        """
        SELECT uniform(CAST(10 AS TINYINT), CAST(20 AS TINYINT), 0) AS result
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: uniform returns short type for smallint inputs
      When query
        """
        SELECT uniform(CAST(100 AS SMALLINT), CAST(200 AS SMALLINT), 0) AS result
        """
      Then query schema
        """
        root
         |-- result: short (nullable = false)
        """

    Scenario: uniform returns float type for float inputs
      When query
        """
        SELECT uniform(CAST(5.5 AS FLOAT), CAST(10.5 AS FLOAT), 123) AS result
        """
      Then query schema
        """
        root
         |-- result: float (nullable = false)
        """
