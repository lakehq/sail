@zeroifnull
Feature: zeroifnull output schema

  @spark_null
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
        | case                            | arg                          | value | type     |
        | zeroifnull doctest #1 (result)  | NULL                         | 0     | int      |
        | zeroifnull doctest #2 (result)  | 2                            | 2     | int      |
        | zeroifnull doctest #3 (result)  | CAST(NULL AS TINYINT)        | 0     | tinyint  |
        | zeroifnull doctest #4 (result)  | CAST(5 AS TINYINT)           | 5     | tinyint  |
        | zeroifnull doctest #5 (result)  | CAST(NULL AS SMALLINT)       | 0     | smallint |
        | zeroifnull doctest #6 (result)  | CAST(NULL AS BIGINT)         | 0     | bigint   |
        | zeroifnull doctest #7 (result)  | CAST(NULL AS FLOAT)          | 0.0   | float    |
        | zeroifnull doctest #8 (result)  | CAST(3.14 AS FLOAT)          | 3.14  | float    |
        | zeroifnull doctest #9 (result)  | CAST(NULL AS DOUBLE)         | 0.0   | double   |
        | zeroifnull doctest #10 (result) | CAST(2.718 AS DOUBLE)        | 2.718 | double   |
        | zeroifnull doctest #11 (result) | CAST(NULL AS DECIMAL(10,2))  | 0.0   | double   |
        | zeroifnull doctest #12 (result) | CAST(42.99 AS DECIMAL(10,2)) | 42.99 | double   |
        | zeroifnull doctest #13 (result) | 0                            | 0     | int      |

    # Kept separate: Spark derives the column name as `zeroifnull((- 5))`, not
    # `zeroifnull(-5)`, so the SQL argument and the header cannot share one slot.
    Scenario: zeroifnull doctest #14 (result)
      When query
        """
        SELECT zeroifnull(-5), typeof(zeroifnull(-5))
        """
      Then query result
        | zeroifnull((- 5)) | typeof(zeroifnull((- 5))) |
        | -5                | int                       |
