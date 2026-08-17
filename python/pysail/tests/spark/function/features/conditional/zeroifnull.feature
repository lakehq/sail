Feature: zeroifnull output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to zeroifnull yields the schema Spark declares
      When query
        """
        SELECT zeroifnull(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Result values (migrated from test_zeroifnull.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT zeroifnull(<arg>), typeof(zeroifnull(<arg>))
        """
      Then query result
        | zeroifnull(<arg>) | typeof(zeroifnull(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                   | value | type   |
        | zeroifnull doctest #1 (result)  | NULL                  | 0     | int    |
        | zeroifnull doctest #2 (result)  | 2                     | 2     | int    |
        | zeroifnull doctest #6 (result)  | CAST(NULL AS BIGINT)  | 0     | bigint |
        | zeroifnull doctest #9 (result)  | CAST(NULL AS DOUBLE)  | 0.0   | double |
        | zeroifnull doctest #10 (result) | CAST(2.718 AS DOUBLE) | 2.718 | double |
        | zeroifnull doctest #13 (result) | 0                     | 0     | int    |

    # `zeroifnull(x)` is `ifnull(x, 0)`, so the result type is the least common type of the
    # argument and the INT literal `0`: TINYINT/SMALLINT widen to INT, FLOAT to DOUBLE, and
    # DECIMAL(10,2) to DECIMAL(12,2). Sail keeps the argument type unchanged.
    @sail-bug
    Scenario Outline: Result values with type promotion against the zero literal: <case>
      When query
        """
        SELECT zeroifnull(<arg>), typeof(zeroifnull(<arg>))
        """
      Then query result
        | zeroifnull(<arg>) | typeof(zeroifnull(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                          | value             | type          |
        | zeroifnull doctest #3 (result)  | CAST(NULL AS TINYINT)        | 0                 | int           |
        | zeroifnull doctest #4 (result)  | CAST(5 AS TINYINT)           | 5                 | int           |
        | zeroifnull doctest #5 (result)  | CAST(NULL AS SMALLINT)       | 0                 | int           |
        | zeroifnull doctest #7 (result)  | CAST(NULL AS FLOAT)          | 0.0               | double        |
        | zeroifnull doctest #8 (result)  | CAST(3.14 AS FLOAT)          | 3.140000104904175 | double        |
        | zeroifnull doctest #11 (result) | CAST(NULL AS DECIMAL(10,2))  | 0.00              | decimal(12,2) |
        | zeroifnull doctest #12 (result) | CAST(42.99 AS DECIMAL(10,2)) | 42.99             | decimal(12,2) |

    # Kept separate because the derived column name repeats the SQL argument, so the two
    # cannot share one Examples slot.
    # Spark derives the name from the literal as written (`-5`); Sail renders it `(- 5)`.
    @sail-bug
    Scenario: zeroifnull doctest #14 (result)
      When query
        """
        SELECT zeroifnull(-5), typeof(zeroifnull(-5))
        """
      Then query result
        | zeroifnull(-5) | typeof(zeroifnull(-5)) |
        | -5             | int                    |
