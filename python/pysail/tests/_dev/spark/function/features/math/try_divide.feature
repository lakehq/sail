Feature: try_divide output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(3, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to try_divide stays nullable
      When query
        """
        SELECT try_divide(c, 2) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Result values (migrated from test_try_divide.txt doctests)

    Scenario: try_divide doctest #1 (result)
      When query
        """
        SELECT try_divide(a, b) AS r FROM VALUES (6000, 15), (1990, 2) AS t(a, b)
        """
      Then query result
        | r     |
        | 400.0 |
        | 995.0 |

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_divide(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                                                  | result                       |
        | try_divide doctest #2 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 2                 | 12 hours                     |
        | try_divide doctest #3 (result)  | make_interval(0, 0, 0, 0, 10, 0, 0), 4                | 2 hours 30 minutes           |
        | try_divide doctest #4 (result)  | make_interval(0, 0, 0, 3, 12, 0, 0), 3                | 1 days 4 hours               |
        | try_divide doctest #5 (result)  | make_interval(0, 0, 0, 0, 0, 0, 10.5), 2              | 5.25 seconds                 |
        | try_divide doctest #6 (result)  | make_interval(0, 0, 1, 0, 0, 0, 0), 2                 | 3 days 12 hours              |
        | try_divide doctest #7 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT) | NULL                         |
        | try_divide doctest #8 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 0                 | NULL                         |
        | try_divide doctest #9 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 2                 | 12 hours                     |
        | try_divide doctest #10 (result) | make_interval(0, 0, 0, -1, 0, 0, 0), 2                | -12 hours                    |
        | try_divide doctest #11 (result) | make_interval(0, 0, 0, 1, 0, 90, 0), 2                | 12 hours 45 minutes          |
        | try_divide doctest #12 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '0-9' YEAR TO MONTH |
        | try_divide doctest #13 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '0-9' YEAR TO MONTH |
        | try_divide doctest #14 (result) | make_interval(0, 0, 0, 0, 0, 0, 1), 2                 | 0.5 seconds                  |
