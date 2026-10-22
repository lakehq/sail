Feature: make_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(CAST(id AS INT), 11, 1, 1, 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    Scenario: a nullable column input to make_interval stays nullable
      When query
        """
        SELECT make_interval(c, 11, 1, 1, 12, 30, 01.001001) AS result FROM VALUES (100), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

  Rule: Result values (migrated from test_make_interval.txt doctests)

    Scenario: make_interval doctest #1 (result)
      When query
        """
        SELECT y, m, w, d, h, mi, s, make_interval(y, m, w, d, h, mi, s) AS interval FROM VALUES (1, 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (CAST(NULL AS INT), 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, CAST(NULL AS INT), 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, CAST(NULL AS INT), 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, CAST(NULL AS INT), 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, CAST(NULL AS INT), 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, CAST(NULL AS INT), CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, 6, CAST(NULL AS DOUBLE)), (1, 1, 1, 1, 1, 1, CAST(1.0 AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('Infinity' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('-Infinity' AS DOUBLE)) AS t(y, m, w, d, h, mi, s)
        """
      Then query result
        | y    | m    | w    | d    | h    | mi   | s         | interval                                               |
        | 1    | 2    | 3    | 4    | 5    | 6    | 7.5       | 1 years 2 months 25 days 5 hours 6 minutes 7.5 seconds |
        | NULL | 2    | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | NULL | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | NULL | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | NULL | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | NULL | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | NULL | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | 6    | NULL      | NULL                                                   |
        | 1    | 1    | 1    | 1    | 1    | 1    | 1.0       | 1 years 1 months 8 days 1 hours 1 minutes 1 seconds    |
        | 0    | 0    | 0    | 0    | 0    | 0    | NaN       | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | Infinity  | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | -Infinity | NULL                                                   |
