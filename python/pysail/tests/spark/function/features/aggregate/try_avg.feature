@try_avg
Feature: try_avg

  Rule: Result values (migrated from test_try_avg.txt doctests)

    Scenario: try_avg doctest #1 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (1), (2), (3) AS t(x)
        """
      Then query result
        | avg_x |
        | 2.0 |

    Scenario: try_avg doctest #3 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(NULL AS INT)), (2), (CAST(NULL AS INT)) AS t(x)
        """
      Then query result
        | avg_x |
        | 2.0 |

    Scenario: try_avg doctest #4 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)) AS t(x)
        """
      Then query result
        | avg_x |
        | 4.611686018427388e18 |

    Scenario: try_avg doctest #5 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) AS t(x)
        """
      Then query result
        | avg_x |
        | 2.3333333333333335 |

    Scenario: try_avg doctest #6 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(1e308 AS DOUBLE)), (CAST(1e308 AS DOUBLE)) AS t(x)
        """
      Then query result
        | avg_x |
        | Infinity |

    Scenario: try_avg doctest #7 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) AS t(x)
        """
      Then query result
        | avg_x |
        | NaN |

    Scenario: try_avg doctest #8 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2))) AS t(x)
        """
      Then query result
        | avg_x |
        | 3.00 |

    Scenario: try_avg doctest #9 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(1.00 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))), (CAST(2.50 AS DECIMAL(10,2))) AS t(x)
        """
      Then query result
        | avg_x |
        | 1.75 |

    Scenario: try_avg doctest #10 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0))) AS t(x)
        """
      Then query result
        | avg_x |
        | 55000 |

    Scenario: try_avg doctest #11 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))) AS t(x)
        """
      Then query result
        | avg_x |
        | NULL |

    Scenario: try_avg doctest #12 (result)
      When query
        """
        SELECT g, try_avg(x) AS avg_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | avg_x |
        | bad | 4.611686018427388e18 |
        | ok | 7.5 |

    Scenario: try_avg doctest #14 (result)
      When query
        """
        SELECT try_avg(col) AS r FROM VALUES (interval '2147483647 months'), (interval '1 months') AS tab(col)
        """
      Then query result
        | r |
        | NULL |

    Scenario: try_avg doctest #15 (result)
      When query
        """
        SELECT try_avg(col) AS r FROM VALUES (interval '7 months'), (interval '1 months') AS tab(col)
        """
      Then query result
        | r |
        | INTERVAL '0-4' YEAR TO MONTH |

    Scenario: try_avg doctest #16 (result)
      When query
        """
        SELECT try_avg(col) AS r FROM VALUES (interval '10 months'), null, (interval '5 months') AS tab(col)
        """
      Then query result
        | r |
        | INTERVAL '0-8' YEAR TO MONTH |

  Rule: Output schema (migrated from test_try_avg.txt printSchema doctests)

    Scenario: try_avg doctest #2 (schema)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (1), (2), (3) AS t(x)
        """
      Then query schema
        """
        root
         |-- avg_x: double (nullable = true)
        """

    Scenario: try_avg doctest #13 (schema)
      When query
        """
        SELECT g, try_avg(x) AS avg_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query schema
        """
        root
         |-- g: string (nullable = true)
         |-- avg_x: double (nullable = true)
        """

