@nullifzero
Feature: nullifzero output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(0) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullifzero stays nullable
      When query
        """
        SELECT nullifzero(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_nullifzero.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT nullifzero(<arg>), typeof(nullifzero(<arg>))
        """
      Then query result
        | nullifzero(<arg>) | typeof(nullifzero(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                         | value | type          |
        | nullifzero doctest #1 (result)  | 0                           | NULL  | int           |
        | nullifzero doctest #2 (result)  | 2                           | 2     | int           |
        | nullifzero doctest #3 (result)  | CAST(0 AS TINYINT)          | NULL  | tinyint       |
        | nullifzero doctest #4 (result)  | CAST(2 AS TINYINT)          | 2     | tinyint       |
        | nullifzero doctest #5 (result)  | CAST(0 AS SMALLINT)         | NULL  | smallint      |
        | nullifzero doctest #6 (result)  | CAST(0 AS BIGINT)           | NULL  | bigint        |
        | nullifzero doctest #7 (result)  | CAST(0.0 AS FLOAT)          | NULL  | float         |
        | nullifzero doctest #8 (result)  | CAST(1.5 AS FLOAT)          | 1.5   | float         |
        | nullifzero doctest #9 (result)  | CAST(0.0 AS DOUBLE)         | NULL  | double        |
        | nullifzero doctest #10 (result) | CAST(2.5 AS DOUBLE)         | 2.5   | double        |
        | nullifzero doctest #11 (result) | CAST(0.00 AS DECIMAL(10,2)) | NULL  | decimal(10,2) |
        | nullifzero doctest #12 (result) | CAST(5.25 AS DECIMAL(10,2)) | 5.25  | decimal(10,2) |
        | nullifzero doctest #14 (result) | NULL                        | NULL  | int           |

    # Kept separate: Spark derives the column name as `nullifzero((- 1))`, not
    # `nullifzero(-1)`, so the SQL argument and the header cannot share one slot.
    Scenario: nullifzero doctest #13 (result)
      When query
        """
        SELECT nullifzero(-1), typeof(nullifzero(-1))
        """
      Then query result
        | nullifzero((- 1)) | typeof(nullifzero((- 1))) |
        | -1                | int                       |
