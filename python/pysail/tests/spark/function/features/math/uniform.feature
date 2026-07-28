@uniform
Feature: uniform() generates random numbers within a range

  # IMPLEMENTATION NOTE:
  # Sail uses `SparkXorShiftRandom` (a port of Apache Spark's XORShiftRandom),
  # so for the same seed the produced values are bit-for-bit identical to
  # Spark JVM — including the first-row value, float/double truncation, and
  # multi-row sequences.

  Rule: Arity validation

    Scenario Outline: Arity: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query error (?i).*

      Examples:
        | case                              | args          |
        | uniform with no arguments fails   |               |
        | uniform with one argument fails   | 10            |
        | uniform with four arguments fails | 1, 10, 42, 99 |

  Rule: Argument type validation

    Scenario Outline: Argument type: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query error (?i).*

      Examples:
        | case                          | args                        |
        | uniform rejects string min    | '1', 10, 0                  |
        | uniform rejects boolean min   | true, false, 0              |
        | uniform rejects string seed   | 1, 10, 'foo'                |
        | uniform rejects decimal seed  | 1, 10, 3.14                 |
        | uniform rejects double seed   | 1, 10, CAST(3.14 AS DOUBLE) |
        | uniform rejects tinyint seed  | 1, 10, CAST(42 AS TINYINT)  |
        | uniform rejects smallint seed | 1, 10, CAST(42 AS SMALLINT) |

    Scenario Outline: Non-foldable argument: <case>
      When query
        """
        SELECT uniform(<args>) AS result FROM range(3)
        """
      Then query error (?i).*

      Examples:
        | case                                          | args                   |
        | uniform rejects non-foldable min from column  | CAST(id AS INT), 10, 0 |
        | uniform rejects non-foldable max from column  | 1, CAST(id AS INT), 0  |
        | uniform rejects non-foldable seed from column | 1, 10, CAST(id AS INT) |

  Rule: Schema type inference for integers

    Scenario Outline: Integer type inference: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = false)
        """

      Examples:
        | case                                                       | args                                            | type    |
        | uniform returns integer type for integer inputs            | 10, 20, 0                                       | integer |
        | uniform returns integer type when no seed provided         | 10, 20                                          | integer |
        | uniform returns byte type for tinyint inputs               | CAST(10 AS TINYINT), CAST(20 AS TINYINT), 0     | byte    |
        | uniform returns short type for smallint inputs             | CAST(100 AS SMALLINT), CAST(200 AS SMALLINT), 0 | short   |
        | uniform returns bigint type for bigint inputs              | CAST(10 AS BIGINT), CAST(20 AS BIGINT), 0       | long    |
        | uniform returns short type for tinyint mixed with smallint | CAST(1 AS TINYINT), CAST(10 AS SMALLINT), 0     | short   |
        | uniform returns int type for smallint mixed with int       | CAST(1 AS SMALLINT), 10, 0                      | integer |
        | uniform returns bigint type for int mixed with bigint      | 1, CAST(10 AS BIGINT), 0                        | long    |
        | uniform returns integer type for INT_MAX bounds            | 2147483647, 2147483647, 0                       | integer |
        | uniform returns long type when exceeding INT_MAX           | 2147483647, 21474836471, 0                      | long    |

  Rule: Schema type inference for floats

    Scenario Outline: Float type inference: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = false)
        """

      Examples:
        | case                                                     | args                                           | type   |
        | uniform returns float type for float inputs              | CAST(5.5 AS FLOAT), CAST(10.5 AS FLOAT), 123   | float  |
        | uniform returns double type for double inputs            | CAST(5.5 AS DOUBLE), CAST(10.5 AS DOUBLE), 123 | double |
        | uniform returns double type for float mixed with double  | CAST(1 AS FLOAT), CAST(10 AS DOUBLE), 0        | double |
        | uniform returns double type for int mixed with double    | 1, CAST(10 AS DOUBLE), 0                       | double |
        | uniform returns double type for bigint mixed with double | CAST(1 AS BIGINT), CAST(10 AS DOUBLE), 0       | double |
        | uniform returns float type for float mixed with int      | CAST(1 AS FLOAT), 10, 0                        | float  |
        | uniform returns float type for float mixed with bigint   | CAST(1 AS FLOAT), CAST(10 AS BIGINT), 0        | float  |

  Rule: Schema type inference for decimals

    Scenario Outline: Decimal type inference: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = false)
        """

      Examples:
        | case                                                       | args                                           | type          |
        | uniform returns decimal type for decimal inputs            | 5.5, 10.5, 123                                 | decimal(3,1)  |
        | uniform returns decimal type for mixed decimal and integer | 5.5, 10, 123                                   | decimal(2,1)  |
        | uniform returns decimal type for mixed integer and decimal | 10, 5.5, 123                                   | decimal(2,1)  |
        | uniform uses larger decimal precision                      | 1, 12345.67890, 42                             | decimal(10,5) |
        | uniform uses decimal scale from input                      | 1, 12.34567890, 42                             | decimal(10,8) |
        | uniform decimal ignores integer type when decimal present  | 1.2, 2147483647, 42                            | decimal(2,1)  |
        | uniform with mixed scales uses larger precision            | 5.65, 100.0, 123                               | decimal(4,1)  |
        | uniform with large int and small decimal uses decimal type | 1234567890, 1.2, 42                            | decimal(2,1)  |
        | uniform returns float type for decimal mixed with float    | CAST(1 AS DECIMAL(5,2)), CAST(10 AS FLOAT), 0  | float         |
        | uniform returns double type for decimal mixed with double  | CAST(1 AS DECIMAL(5,2)), CAST(10 AS DOUBLE), 0 | double        |

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

  Rule: All-null short-circuit must NOT bypass seed validation

    # Invariant: the all-null fast path in `uniform()` lives AFTER `coerce_types`,
    # so invalid-seed errors must still fire even when a bound is NULL. Moving
    # the short-circuit above validation (or duplicating validation to a later
    # stage) would flip these from error to pass (silent bug).

    Scenario Outline: NULL bound plus bad seed: <case>
      When query
        """
        SELECT uniform(NULL, 10, <seed>) AS result
        """
      Then query error (?i).*

      Examples:
        | case                                                   | seed                |
        | uniform with NULL bound plus STRING seed still errors  | 'foo'               |
        | uniform with NULL bound plus DECIMAL seed still errors | 3.14                |
        | uniform with NULL bound plus TINYINT seed still errors | CAST(42 AS TINYINT) |

  Rule: NULL handling

    Scenario Outline: NULL handling: <case>
      When query
        """
        SELECT CAST(uniform(<args>) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                  | args          |
        | uniform result is NULL when min is NULL               | NULL, 10, 0   |
        | uniform result is NULL when max is NULL               | 1, NULL, 0    |
        | uniform result is NULL when both min and max are NULL | NULL, NULL, 0 |

  Rule: Multi-row behavior

    Scenario: uniform on empty batch returns an empty result
      # Guards against a panic from extracting the seed at row 0 of a
      # zero-length array when number_rows == 0.
      When query
        """
        SELECT uniform(1, 10, 0) AS result FROM range(0)
        """
      Then query result
        | result |

    Scenario: uniform with equal bounds produces the same value for every row
      When query
        """
        SELECT uniform(5, 5, 0) AS result FROM range(3)
        """
      Then query result
        | result |
        | 5      |
        | 5      |
        | 5      |

    Scenario: uniform with NULL min produces NULL for every row
      When query
        """
        SELECT CAST(uniform(NULL, 10, 0) AS STRING) AS result FROM range(3)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: uniform with seed produces varied values across rows
      When query
        """
        WITH x AS (SELECT uniform(0, 1000, 42) AS v FROM range(50))
        SELECT COUNT(DISTINCT v) > 1 AS has_variation FROM x
        """
      Then query result
        | has_variation |
        | true          |

  Rule: Bit-exact values match Spark JVM

    Scenario Outline: Bit-exact: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                    | args                                    | result             |
        | uniform int with seed 0 matches Spark                   | 10, 20, 0                               | 17                 |
        | uniform int with seed 42 matches Spark                  | 0, 100, 42                              | 61                 |
        | uniform int with negative seed matches Spark            | 5, 105, -3                              | 81                 |
        | uniform double with seed 0 matches Spark                | CAST(1 AS DOUBLE), CAST(2 AS DOUBLE), 0 | 1.7604953758285915 |
        | uniform float with seed 0 matches Spark                 | CAST(1 AS FLOAT), CAST(2 AS FLOAT), 0   | 1.7604954          |
        | uniform decimal with seed 123 matches Spark             | 5.5, 10.5, 123                          | 6.3                |
        | uniform with swapped bounds matches Spark (large range) | 1000, 0, 42                             | 380                |

    Scenario: uniform int with seed 42 multi-row matches Spark
      When query
        """
        SELECT uniform(0, 1000, 42) AS result FROM range(5)
        """
      Then query result
        | result |
        | 619    |
        | 509    |
        | 832    |
        | 263    |
        | 670    |

    Scenario: uniform int with seed 0 multi-row matches Spark
      When query
        """
        SELECT uniform(0, 1000, 0) AS result FROM range(5)
        """
      Then query result
        | result |
        | 760    |
        | 523    |
        | 95     |
        | 316    |
        | 714    |

    Scenario: uniform with swapped bounds matches Spark (int)
      # Spark does NOT normalize min/max. `uniform(20, 10, 0)` uses a negative
      # span and lands in (max, min] rather than [min, max).
      When query
        """
        SELECT uniform(20, 10, 0) AS result
        """
      Then query result
        | result |
        | 12     |

  Rule: Equal bounds are deterministic across RNGs

    Scenario Outline: Equal bounds: <case>
      When query
        """
        SELECT uniform(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | args                                      | result     |
        | uniform returns the shared bound when min equals max     | 5, 5, 0                                   | 5          |
        | uniform returns 0 when both bounds are 0                 | 0, 0, 42                                  | 0          |
        | uniform returns negative shared bound                    | -10, -10, 7                               | -10        |
        | uniform returns INT_MAX when both bounds are INT_MAX     | 2147483647, 2147483647, 0                 | 2147483647 |
        | uniform returns shared decimal bound when min equals max | 5.5, 5.5, 0                               | 5.5        |
        | uniform returns shared float bound when min equals max   | CAST(2.5 AS FLOAT), CAST(2.5 AS FLOAT), 0 | 2.5        |
