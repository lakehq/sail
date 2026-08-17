Feature: try_multiply output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_multiply stays nullable
      When query
        """
        SELECT try_multiply(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_multiply.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_multiply(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | args                                                  | result                       |
        | try_multiply doctest #1 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 2                 | 2 days                       |
        | try_multiply doctest #2 (result)  | make_interval(0, 0, 0, 0, 10, 0, 0), 4                | 40 hours                     |
        | try_multiply doctest #3 (result)  | make_interval(0, 0, 0, 3, 12, 0, 0), 3                | 9 days 36 hours              |
        | try_multiply doctest #4 (result)  | make_interval(0, 0, 0, 0, 0, 0, 10.5), 2              | 21 seconds                   |
        | try_multiply doctest #5 (result)  | make_interval(0, 0, 1, 0, 0, 0, 0), 2                 | 14 days                      |
        | try_multiply doctest #6 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT) | NULL                         |
        | try_multiply doctest #7 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 0                 | 0 seconds                    |
        | try_multiply doctest #8 (result)  | make_interval(0, 0, 0, -1, 0, 0, 0), 2                | -2 days                      |
        | try_multiply doctest #9 (result)  | make_interval(0, 0, 0, 1, 0, 90, 0), 2                | 2 days 3 hours               |
        | try_multiply doctest #10 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '3-0' YEAR TO MONTH |
        | try_multiply doctest #11 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '3-0' YEAR TO MONTH |
