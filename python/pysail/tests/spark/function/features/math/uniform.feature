Feature: uniform() generates random numbers within a range

  # IMPLEMENTATION NOTE:
  # Sail uses Rust's StdRng (ChaCha20-based RNG) while Spark uses Java's Random (LCG),
  # so specific numeric output differs by seed. These scenarios therefore cover only
  # behavior that is deterministic across RNGs: schema-type inference, NULL handling,
  # equal-bound short-circuit, and error-condition validation.

  Rule: Arity validation

    Scenario: uniform with no arguments fails
      When query
        """
        SELECT uniform() AS result
        """
      Then query error (?i).*

    Scenario: uniform with one argument fails
      When query
        """
        SELECT uniform(10) AS result
        """
      Then query error (?i).*

    Scenario: uniform with four arguments fails
      When query
        """
        SELECT uniform(1, 10, 42, 99) AS result
        """
      Then query error (?i).*

  Rule: Argument type validation

    Scenario: uniform rejects string min
      When query
        """
        SELECT uniform('1', 10, 0) AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects boolean min
      When query
        """
        SELECT uniform(true, false, 0) AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects non-foldable min from column
      When query
        """
        SELECT uniform(CAST(id AS INT), 10, 0) AS result FROM range(3)
        """
      Then query error (?i).*

    Scenario: uniform rejects non-foldable max from column
      When query
        """
        SELECT uniform(1, CAST(id AS INT), 0) AS result FROM range(3)
        """
      Then query error (?i).*

    Scenario: uniform rejects non-foldable seed from column
      When query
        """
        SELECT uniform(1, 10, CAST(id AS INT)) AS result FROM range(3)
        """
      Then query error (?i).*

    Scenario: uniform rejects string seed
      When query
        """
        SELECT uniform(1, 10, 'foo') AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects decimal seed
      When query
        """
        SELECT uniform(1, 10, 3.14) AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects double seed
      When query
        """
        SELECT uniform(1, 10, CAST(3.14 AS DOUBLE)) AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects tinyint seed
      When query
        """
        SELECT uniform(1, 10, CAST(42 AS TINYINT)) AS result
        """
      Then query error (?i).*

    Scenario: uniform rejects smallint seed
      When query
        """
        SELECT uniform(1, 10, CAST(42 AS SMALLINT)) AS result
        """
      Then query error (?i).*

  Rule: Schema type inference for integers

    Scenario: uniform returns integer type for integer inputs
      When query
        """
        SELECT uniform(10, 20, 0) AS result
        """
      Then query schema type
        | column | type | nullable |
        | result | int  | false    |

    Scenario: uniform returns integer type when no seed provided
      When query
        """
        SELECT uniform(10, 20) AS result
        """
      Then query schema type
        | column | type | nullable |
        | result | int  | false    |

    Scenario: uniform returns byte type for tinyint inputs
      When query
        """
        SELECT uniform(CAST(10 AS TINYINT), CAST(20 AS TINYINT), 0) AS result
        """
      Then query schema type
        | column | type    | nullable |
        | result | tinyint | false    |

    Scenario: uniform returns short type for smallint inputs
      When query
        """
        SELECT uniform(CAST(100 AS SMALLINT), CAST(200 AS SMALLINT), 0) AS result
        """
      Then query schema type
        | column | type     | nullable |
        | result | smallint | false    |

    Scenario: uniform returns bigint type for bigint inputs
      When query
        """
        SELECT uniform(CAST(10 AS BIGINT), CAST(20 AS BIGINT), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | bigint | false    |

    Scenario: uniform returns short type for tinyint mixed with smallint
      When query
        """
        SELECT uniform(CAST(1 AS TINYINT), CAST(10 AS SMALLINT), 0) AS result
        """
      Then query schema type
        | column | type     | nullable |
        | result | smallint | false    |

    Scenario: uniform returns int type for smallint mixed with int
      When query
        """
        SELECT uniform(CAST(1 AS SMALLINT), 10, 0) AS result
        """
      Then query schema type
        | column | type | nullable |
        | result | int  | false    |

    Scenario: uniform returns bigint type for int mixed with bigint
      When query
        """
        SELECT uniform(1, CAST(10 AS BIGINT), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | bigint | false    |

    Scenario: uniform returns integer type for INT_MAX bounds
      When query
        """
        SELECT uniform(2147483647, 2147483647, 0) AS result
        """
      Then query schema type
        | column | type | nullable |
        | result | int  | false    |

    Scenario: uniform returns long type when exceeding INT_MAX
      When query
        """
        SELECT uniform(2147483647, 21474836471, 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | bigint | false    |

  Rule: Schema type inference for floats

    Scenario: uniform returns float type for float inputs
      When query
        """
        SELECT uniform(CAST(5.5 AS FLOAT), CAST(10.5 AS FLOAT), 123) AS result
        """
      Then query schema type
        | column | type  | nullable |
        | result | float | false    |

    Scenario: uniform returns double type for double inputs
      When query
        """
        SELECT uniform(CAST(5.5 AS DOUBLE), CAST(10.5 AS DOUBLE), 123) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | double | false    |

    Scenario: uniform returns double type for float mixed with double
      When query
        """
        SELECT uniform(CAST(1 AS FLOAT), CAST(10 AS DOUBLE), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | double | false    |

    Scenario: uniform returns double type for int mixed with double
      When query
        """
        SELECT uniform(1, CAST(10 AS DOUBLE), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | double | false    |

    Scenario: uniform returns double type for bigint mixed with double
      When query
        """
        SELECT uniform(CAST(1 AS BIGINT), CAST(10 AS DOUBLE), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | double | false    |

    Scenario: uniform returns float type for float mixed with int
      When query
        """
        SELECT uniform(CAST(1 AS FLOAT), 10, 0) AS result
        """
      Then query schema type
        | column | type  | nullable |
        | result | float | false    |

    Scenario: uniform returns float type for float mixed with bigint
      When query
        """
        SELECT uniform(CAST(1 AS FLOAT), CAST(10 AS BIGINT), 0) AS result
        """
      Then query schema type
        | column | type  | nullable |
        | result | float | false    |

  Rule: Schema type inference for decimals

    Scenario: uniform returns decimal type for decimal inputs
      When query
        """
        SELECT uniform(5.5, 10.5, 123) AS result
        """
      Then query schema type
        | column | type         | nullable |
        | result | decimal(3,1) | false    |

    Scenario: uniform returns decimal type for mixed decimal and integer
      When query
        """
        SELECT uniform(5.5, 10, 123) AS result
        """
      Then query schema type
        | column | type         | nullable |
        | result | decimal(2,1) | false    |

    Scenario: uniform returns decimal type for mixed integer and decimal
      When query
        """
        SELECT uniform(10, 5.5, 123) AS result
        """
      Then query schema type
        | column | type         | nullable |
        | result | decimal(2,1) | false    |

    Scenario: uniform uses larger decimal precision
      When query
        """
        SELECT uniform(1, 12345.67890, 42) AS result
        """
      Then query schema type
        | column | type          | nullable |
        | result | decimal(10,5) | false    |

    Scenario: uniform uses decimal scale from input
      When query
        """
        SELECT uniform(1, 12.34567890, 42) AS result
        """
      Then query schema type
        | column | type          | nullable |
        | result | decimal(10,8) | false    |

    Scenario: uniform with large decimal precision
      When query
        """
        SELECT uniform(
          1.2,
          12345678901234567890,
          42
        ) AS result
        """
      Then query schema type
        | column | type          | nullable |
        | result | decimal(20,0) | false    |

    Scenario: uniform decimal ignores integer type when decimal present
      When query
        """
        SELECT uniform(1.2, 2147483647, 42) AS result
        """
      Then query schema type
        | column | type         | nullable |
        | result | decimal(2,1) | false    |

    Scenario: uniform decimal ignores bigint type when decimal present
      When query
        """
        SELECT uniform(
          1.2,
          CAST(9223372036854775807 AS BIGINT),
          43
        ) AS result
        """
      Then query schema type
        | column | type         | nullable |
        | result | decimal(2,1) | false    |

    Scenario: uniform returns float type for decimal mixed with float
      When query
        """
        SELECT uniform(CAST(1 AS DECIMAL(5,2)), CAST(10 AS FLOAT), 0) AS result
        """
      Then query schema type
        | column | type  | nullable |
        | result | float | false    |

    Scenario: uniform returns double type for decimal mixed with double
      When query
        """
        SELECT uniform(CAST(1 AS DECIMAL(5,2)), CAST(10 AS DOUBLE), 0) AS result
        """
      Then query schema type
        | column | type   | nullable |
        | result | double | false    |

  Rule: NULL handling

    # NOTE: Spark declares the field as nullable=false even for literal NULL bounds
    # (constant-folding decides at plan time). Sail's Arrow layer enforces that
    # non-nullable arrays may not contain nulls, so we drop the nullable assertion
    # here and let each implementation choose what best matches its runtime.
    Scenario: uniform returns double type when min is NULL
      When query
        """
        SELECT uniform(NULL, 10, 0) AS result
        """
      Then query schema type
        | column | type   |
        | result | double |

    Scenario: uniform returns double type when max is NULL
      When query
        """
        SELECT uniform(1, NULL, 0) AS result
        """
      Then query schema type
        | column | type   |
        | result | double |

    Scenario: uniform returns double type when both min and max are NULL
      When query
        """
        SELECT uniform(NULL, NULL, 0) AS result
        """
      Then query schema type
        | column | type   |
        | result | double |

    Scenario: uniform result is NULL when min is NULL
      When query
        """
        SELECT CAST(uniform(NULL, 10, 0) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: uniform result is NULL when max is NULL
      When query
        """
        SELECT CAST(uniform(1, NULL, 0) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: uniform result is NULL when both min and max are NULL
      When query
        """
        SELECT CAST(uniform(NULL, NULL, 0) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Equal bounds are deterministic across RNGs

    Scenario: uniform returns the shared bound when min equals max
      When query
        """
        SELECT uniform(5, 5, 0) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: uniform returns 0 when both bounds are 0
      When query
        """
        SELECT uniform(0, 0, 42) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: uniform returns negative shared bound
      When query
        """
        SELECT uniform(-10, -10, 7) AS result
        """
      Then query result
        | result |
        | -10    |

    Scenario: uniform returns INT_MAX when both bounds are INT_MAX
      When query
        """
        SELECT uniform(2147483647, 2147483647, 0) AS result
        """
      Then query result
        | result     |
        | 2147483647 |

    Scenario: uniform returns shared decimal bound when min equals max
      When query
        """
        SELECT uniform(5.5, 5.5, 0) AS result
        """
      Then query result
        | result |
        | 5.5    |

    Scenario: uniform returns shared float bound when min equals max
      When query
        """
        SELECT uniform(CAST(2.5 AS FLOAT), CAST(2.5 AS FLOAT), 0) AS result
        """
      Then query result
        | result |
        | 2.5    |

