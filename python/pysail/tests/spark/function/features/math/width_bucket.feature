@width_bucket
Feature: width_bucket output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to width_bucket yields the schema Spark declares
      When query
        """
        SELECT width_bucket(5.3, 0.2, 10.6, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable column input to width_bucket stays nullable
      When query
        """
        SELECT width_bucket(c, 0.2, 10.6, 5) AS result FROM VALUES (5.3), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: Result values (migrated from test_width_bucket.txt doctests)

    Scenario: width_bucket doctest #1 (result)
      When query
        """
        SELECT width_bucket(v, lo, hi, n) AS result FROM VALUES (CAST(0.00 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(10.00 AS DECIMAL(10,2)), 5), (CAST(2.00 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(10.00 AS DECIMAL(10,2)), 5), (CAST(10.00 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(10.00 AS DECIMAL(10,2)), 5), (CAST(10.01 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(10.00 AS DECIMAL(10,2)), 5), (CAST(-0.01 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(10.00 AS DECIMAL(10,2)), 5) AS t(v, lo, hi, n)
        """
      Then query result
        | result |
        | 1      |
        | 2      |
        | 6      |
        | 6      |
        | 0      |

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT width_bucket(<args>)
        """
      Then query result
        | width_bucket(<args>) |
        | <result>             |

      Examples:
        | case                                       | args                 | result |
        | width_bucket returns middle bucket         | 5.0, 0.0, 10.0, 5    | 3      |
        | width_bucket doctest #2 (result)           | 0.0, 10.0, 0.0, 5    | 6      |
        | width_bucket doctest #3 (result)           | 10.0, 0.0, 10.0, 5   | 6      |
        | width_bucket doctest #4 (result)           | 10.0, 0.0, 0.0, 5    | NULL   |
