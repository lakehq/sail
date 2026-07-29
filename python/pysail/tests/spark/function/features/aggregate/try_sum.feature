@try_sum
Feature: try_sum

  Rule: Result values (migrated from test_try_sum.txt doctests)

    Scenario Outline: try_sum doctest <case> (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES <values> AS t(x)
        """
      Then query result
        | sum_x   |
        | <sum_x> |

      Examples:
        | case | values                                                                                      | sum_x    |
        | #1   | (1), (2), (3)                                                                               | 6        |
        | #2   | (CAST(NULL AS INT)), (2), (CAST(NULL AS INT))                                               | 2        |
        | #3   | (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT))                                  | NULL     |
        | #4   | (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)), (CAST(3.0 AS DOUBLE))                         | 7.0      |
        | #5   | (CAST(1e308 AS DOUBLE)), (CAST(1e308 AS DOUBLE))                                            | Infinity |
        | #6   | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE))                                              | NaN      |
        | #7   | (CAST('Infinity' AS DOUBLE)), (CAST(1.0 AS DOUBLE))                                         | Infinity |
        | #8   | (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2)))                                | 6.00     |
        | #10  | (CAST(1.00 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))), (CAST(2.50 AS DECIMAL(10,2))) | 3.50     |
        | #11  | (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0)))                                | 110000   |

    Scenario: try_sum doctest #13 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))) AS t(x)
        """
      Then query result
        | sum_x |
        | NULL  |

    Scenario: try_sum doctest #15 (result)
      When query
        """
        SELECT g, try_sum(x) AS sum_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g   | sum_x |
        | bad | NULL  |
        | ok  | 15    |

  Rule: Output schema (migrated from test_try_sum.txt printSchema doctests)

    Scenario Outline: try_sum doctest <case> (schema)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES <values> AS t(x)
        """
      Then query schema
        """
        root
         |-- sum_x: <type> (nullable = true)
        """

      Examples:
        | case | values                                                       | type          |
        | #9   | (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2))) | decimal(20,2) |
        | #12  | (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0))) | decimal(15,0) |

    Scenario: try_sum doctest #14 (schema)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))) AS t(x)
        """
      Then query schema
        """
        root
         |-- sum_x: decimal(38,0) (nullable = true)
        """

    Scenario: try_sum doctest #16 (schema)
      When query
        """
        SELECT g, try_sum(x) AS sum_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query schema
        """
        root
         |-- g: string (nullable = true)
         |-- sum_x: long (nullable = true)
        """
