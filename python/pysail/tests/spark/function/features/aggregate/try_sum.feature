@try_sum
Feature: try_sum

  Rule: Result values (migrated from test_try_sum.txt doctests)

    Scenario: try_sum doctest #1 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (1), (2), (3) AS t(x)
        """
      Then query result
        | sum_x |
        | 6 |

    Scenario: try_sum doctest #2 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(NULL AS INT)), (2), (CAST(NULL AS INT)) AS t(x)
        """
      Then query result
        | sum_x |
        | 2 |

    Scenario: try_sum doctest #3 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)) AS t(x)
        """
      Then query result
        | sum_x |
        | NULL |

    Scenario: try_sum doctest #4 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) AS t(x)
        """
      Then query result
        | sum_x |
        | 7.0 |

    Scenario: try_sum doctest #5 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(1e308 AS DOUBLE)), (CAST(1e308 AS DOUBLE)) AS t(x)
        """
      Then query result
        | sum_x |
        | Infinity |

    Scenario: try_sum doctest #6 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) AS t(x)
        """
      Then query result
        | sum_x |
        | NaN |

    Scenario: try_sum doctest #7 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST('Infinity' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) AS t(x)
        """
      Then query result
        | sum_x |
        | Infinity |

    Scenario: try_sum doctest #8 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2))) AS t(x)
        """
      Then query result
        | sum_x |
        | 6.00 |

    Scenario: try_sum doctest #10 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(1.00 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))), (CAST(2.50 AS DECIMAL(10,2))) AS t(x)
        """
      Then query result
        | sum_x |
        | 3.50 |

    Scenario: try_sum doctest #11 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0))) AS t(x)
        """
      Then query result
        | sum_x |
        | 110000 |

    Scenario: try_sum doctest #13 (result)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))) AS t(x)
        """
      Then query result
        | sum_x |
        | NULL |

    Scenario: try_sum doctest #15 (result)
      When query
        """
        SELECT g, try_sum(x) AS sum_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | sum_x |
        | bad | NULL |
        | ok | 15 |

  Rule: Output schema (migrated from test_try_sum.txt printSchema doctests)

    Scenario: try_sum doctest #9 (schema)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2))) AS t(x)
        """
      Then query schema
        """
        root
         |-- sum_x: decimal(20,2) (nullable = true)
        """

    Scenario: try_sum doctest #12 (schema)
      When query
        """
        SELECT try_sum(x) AS sum_x FROM VALUES (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0))) AS t(x)
        """
      Then query schema
        """
        root
         |-- sum_x: decimal(15,0) (nullable = true)
        """

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

