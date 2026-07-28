@try_mod
Feature: try_mod output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_mod yields the schema Spark declares
      When query
        """
        SELECT try_mod(3, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_mod yields the schema Spark declares
      When query
        """
        SELECT try_mod(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_mod stays nullable
      When query
        """
        SELECT try_mod(c, 2) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_mod.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_mod(<args>) AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                         | args                                                                                    | result               |
        | try_mod doctest #1 (result)  | CAST(3.0  AS DECIMAL(2,1)), CAST(2.00 AS DECIMAL(3,2))                                  | 1.00                 |
        | try_mod doctest #2 (result)  | CAST(12.34 AS DECIMAL(4,2)), CAST(5.67 AS DECIMAL(3,2))                                 | 1.00                 |
        | try_mod doctest #3 (result)  | CAST(1.2 AS DECIMAL(2,1)), CAST(0.03 AS DECIMAL(3,2))                                   | 0.00                 |
        | try_mod doctest #4 (result)  | CAST(1.23 AS DECIMAL(3,2)), CAST(0.00 AS DECIMAL(3,2))                                  | NULL                 |
        | try_mod doctest #5 (result)  | CAST(0.00 AS DECIMAL(3,2)), CAST(2.50 AS DECIMAL(3,2))                                  | 0.00                 |
        | try_mod doctest #6 (result)  | CAST(1.23 AS DECIMAL(3,2)), CAST(9.99 AS DECIMAL(3,2))                                  | 1.23                 |
        | try_mod doctest #7 (result)  | CAST(5.00 AS DECIMAL(3,2)), CAST(5.00 AS DECIMAL(3,2))                                  | 0.00                 |
        | try_mod doctest #8 (result)  | CAST(9.9 AS DECIMAL(38,1)), CAST(0.000000000000000001 AS DECIMAL(18,18))                | 0.000000000000000000 |
        | try_mod doctest #9 (result)  | CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), CAST(10 AS DECIMAL(2,0)) | 9                    |
        | try_mod doctest #10 (result) | 3, CAST(2.00 AS DECIMAL(3,2))                                                           | 1.00                 |
        | try_mod doctest #11 (result) | CAST(3.00 AS DECIMAL(3,2)), 2                                                           | 1.00                 |
        | try_mod doctest #12 (result) | NULL, CAST(2.00 AS DECIMAL(3,2))                                                        | NULL                 |
        | try_mod doctest #13 (result) | CAST(3.00 AS DECIMAL(3,2)), NULL                                                        | NULL                 |
        | try_mod doctest #14 (result) | CAST(1.23 AS DECIMAL(3,2)), CAST(0.000000000001 AS DECIMAL(13,12))                      | 0.000000000000       |
        | try_mod doctest #15 (result) | CAST(10.00 AS DECIMAL(4,2)), CAST(2.50 AS DECIMAL(3,2))                                 | 0.00                 |

    Scenario: try_mod doctest #16 (result)
      When query
        """
        SELECT
          try_mod(CAST( 7.5 AS DECIMAL(3,1)), CAST( 2.0 AS DECIMAL(2,1))) AS p_p,
          try_mod(CAST(-7.5 AS DECIMAL(3,1)), CAST( 2.0 AS DECIMAL(2,1))) AS n_p,
          try_mod(CAST( 7.5 AS DECIMAL(3,1)), CAST(-2.0 AS DECIMAL(2,1))) AS p_n,
          try_mod(CAST(-7.5 AS DECIMAL(3,1)), CAST(-2.0 AS DECIMAL(2,1))) AS n_n
        """
      Then query result
        | p_p | n_p  | p_n | n_n  |
        | 1.5 | -1.5 | 1.5 | -1.5 |

  Rule: Output schema (migrated from test_try_mod.txt printSchema doctests)

    Scenario: try_mod doctest #17 (schema)
      When query
        """
        SELECT try_mod(CAST(3.0  AS DECIMAL(2,1)), CAST(2.00 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #18 (schema)
      When query
        """
        SELECT try_mod(CAST(12.34 AS DECIMAL(4,2)), CAST(5.67 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #19 (schema)
      When query
        """
        SELECT try_mod(CAST(1.2 AS DECIMAL(2,1)), CAST(0.03 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #20 (schema)
      When query
        """
        SELECT try_mod(CAST(1.23 AS DECIMAL(3,2)), CAST(0.00 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #21 (schema)
      When query
        """
        SELECT try_mod(CAST(0.00 AS DECIMAL(3,2)), CAST(2.50 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #22 (schema)
      When query
        """
        SELECT try_mod(CAST(1.23 AS DECIMAL(3,2)), CAST(9.99 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #23 (schema)
      When query
        """
        SELECT try_mod(CAST(5.00 AS DECIMAL(3,2)), CAST(5.00 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #24 (schema)
      When query
        """
        SELECT try_mod(CAST(9.9 AS DECIMAL(38,1)), CAST(0.000000000000000001 AS DECIMAL(18,18))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(18,18) (nullable = true)
        """

    Scenario: try_mod doctest #25 (schema)
      When query
        """
        SELECT try_mod(CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)), CAST(10 AS DECIMAL(2,0))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(2,0) (nullable = true)
        """

    Scenario: try_mod doctest #26 (schema)
      When query
        """
        SELECT try_mod(3, CAST(2.00 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #27 (schema)
      When query
        """
        SELECT try_mod(CAST(3.00 AS DECIMAL(3,2)), 2) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #28 (schema)
      When query
        """
        SELECT try_mod(NULL, CAST(2.00 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #29 (schema)
      When query
        """
        SELECT try_mod(CAST(3.00 AS DECIMAL(3,2)), NULL) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #30 (schema)
      When query
        """
        SELECT try_mod(CAST(1.23 AS DECIMAL(3,2)), CAST(0.000000000001 AS DECIMAL(13,12))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(13,12) (nullable = true)
        """

    Scenario: try_mod doctest #31 (schema)
      When query
        """
        SELECT try_mod(CAST(10.00 AS DECIMAL(4,2)), CAST(2.50 AS DECIMAL(3,2))) AS r
        """
      Then query schema
        """
        root
         |-- r: decimal(3,2) (nullable = true)
        """

    Scenario: try_mod doctest #32 (schema)
      When query
        """
        SELECT
          try_mod(CAST( 7.5 AS DECIMAL(3,1)), CAST( 2.0 AS DECIMAL(2,1))) AS p_p,
          try_mod(CAST(-7.5 AS DECIMAL(3,1)), CAST( 2.0 AS DECIMAL(2,1))) AS n_p,
          try_mod(CAST( 7.5 AS DECIMAL(3,1)), CAST(-2.0 AS DECIMAL(2,1))) AS p_n,
          try_mod(CAST(-7.5 AS DECIMAL(3,1)), CAST(-2.0 AS DECIMAL(2,1))) AS n_n
        """
      Then query schema
        """
        root
         |-- p_p: decimal(2,1) (nullable = true)
         |-- n_p: decimal(2,1) (nullable = true)
         |-- p_n: decimal(2,1) (nullable = true)
         |-- n_n: decimal(2,1) (nullable = true)
        """
