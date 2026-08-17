Feature: try_add output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_add stays nullable
      When query
        """
        SELECT try_add(c, 2) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_add.txt doctests)

    Scenario: try_add doctest #1 (result)
      When query
        """
        SELECT birth, age, try_add(birth, age) AS birth_plus_age FROM VALUES (CAST(1982 AS INT), CAST(15 AS INT)), (CAST(1990 AS INT), CAST(2 AS INT)), (CAST(NULL AS INT), CAST(10 AS INT)), (CAST(2147483647 AS INT), CAST(1 AS INT)), (CAST(-2147483648 AS INT), CAST(-1 AS INT)) AS t(birth, age)
        """
      Then query result
        | birth       | age | birth_plus_age |
        | 1982        | 15  | 1997           |
        | 1990        | 2   | 1992           |
        | NULL        | 10  | NULL           |
        | 2147483647  | 1   | NULL           |
        | -2147483648 | -1  | NULL           |

    Scenario: try_add doctest #2 (result)
      When query
        """
        SELECT try_add(DATE '2015-09-30', 1) as d1, try_add(DATE '2000-01-01', 366) as d2, try_add(DATE '2021-01-01', 1) as d3, try_add(NULL, 100) as d4
        """
      Then query result
        | d1         | d2         | d3         | d4   |
        | 2015-10-01 | 2001-01-01 | 2021-01-02 | NULL |

    Scenario: try_add doctest #3 (result)
      When query
        """
        SELECT try_add(DATE '2015-01-31', INTERVAL 1 MONTH) as d1, try_add(DATE '2020-02-29', INTERVAL 12 MONTH) as d2, try_add(NULL, INTERVAL 3 MONTH) as d3
        """
      Then query result
        | d1         | d2         | d3   |
        | 2015-02-28 | 2021-02-28 | NULL |

    Scenario: try_add doctest #4 (result)
      When query
        """
        SELECT try_add(DATE '2000-07-31', INTERVAL -1 MONTH) as d1, try_add(DATE '2021-01-31', INTERVAL -1 MONTH) as d2
        """
      Then query result
        | d1         | d2         |
        | 2000-06-30 | 2020-12-31 |

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_add(<args>) as result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | args                                            | result              |
        | try_add doctest #6 (result) | TIMESTAMP '2021-01-01 00:00:00', INTERVAL 1 DAY | 2021-01-02 00:00:00 |

    # Adding two YEAR intervals keeps the YEAR-only field range in Spark, which renders as
    # `INTERVAL '3' YEAR`. Sail widens the result to YEAR TO MONTH and renders `'3-0'`.
    @sail-bug
    Scenario: try_add doctest #5 (result)
      When query
        """
        SELECT try_add(INTERVAL '1' YEAR, INTERVAL '2' YEAR) as result
        """
      Then query result
        | result            |
        | INTERVAL '3' YEAR |
