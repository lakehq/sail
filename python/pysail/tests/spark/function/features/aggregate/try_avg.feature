Feature: try_avg

  Rule: Result values (migrated from test_try_avg.txt doctests)

    Scenario Outline: try_avg doctest <case> (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES <values> AS t(x)
        """
      Then query result
        | avg_x   |
        | <avg_x> |

      Examples:
        | case | values                                                              | avg_x              |
        | #1   | (1), (2), (3)                                                       | 2.0                |
        | #3   | (CAST(NULL AS INT)), (2), (CAST(NULL AS INT))                       | 2.0                |
        | #5   | (CAST(1.5 AS DOUBLE)), (CAST(2.5 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) | 2.3333333333333335 |
        | #6   | (CAST(1e308 AS DOUBLE)), (CAST(1e308 AS DOUBLE))                    | Infinity           |
        | #7   | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE))                      | NaN                |

    # `avg` over DECIMAL(p, s) yields DECIMAL(p + 4, s + 4), so the rendered value carries the
    # wider scale. Sail keeps the input scale.
    @sail-bug
    Scenario Outline: Result values widening the decimal scale: <case>
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES <values> AS t(x)
        """
      Then query result
        | avg_x   |
        | <avg_x> |

      Examples:
        | case | values                                                                                      | avg_x      |
        | #8   | (CAST(1.23 AS DECIMAL(10,2))), (CAST(4.77 AS DECIMAL(10,2)))                                | 3.000000   |
        | #9   | (CAST(1.00 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))), (CAST(2.50 AS DECIMAL(10,2))) | 1.750000   |
        | #10  | (CAST(90000 AS DECIMAL(5,0))), (CAST(20000 AS DECIMAL(5,0)))                                | 55000.0000 |

    # Spark renders doubles with `java.lang.Double.toString` (`4.6116860184273879E18`);
    # Sail renders `4.611686018427388e18`. Same value, different show-string.
    @sail-bug
    Scenario Outline: Result values rendering a large double: <case>
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES <values> AS t(x)
        """
      Then query result
        | avg_x   |
        | <avg_x> |

      Examples:
        | case | values                                                     | avg_x                 |
        | #4   | (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)) | 4.6116860184273879E18 |

    Scenario: try_avg doctest #11 (result)
      When query
        """
        SELECT try_avg(x) AS avg_x FROM VALUES (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))), (CAST('11111111111111111111111111111111111111' AS DECIMAL(38,0))) AS t(x)
        """
      Then query result
        | avg_x |
        | NULL  |

    @sail-bug
    Scenario: try_avg doctest #12 (result)
      When query
        """
        SELECT g, try_avg(x) AS avg_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g   | avg_x                 |
        | bad | 4.6116860184273879E18 |
        | ok  | 7.5                   |

    Scenario Outline: try_avg doctest <case> over year-month intervals (result)
      When query
        """
        SELECT try_avg(col) AS r FROM VALUES <values> AS tab(col)
        """
      Then query result
        | r   |
        | <r> |

      Examples:
        | case | values                                                | r                            |
        | #14  | (interval '2147483647 months'), (interval '1 months') | NULL                         |
        | #15  | (interval '7 months'), (interval '1 months')          | INTERVAL '0-4' YEAR TO MONTH |
        | #16  | (interval '10 months'), null, (interval '5 months')   | INTERVAL '0-8' YEAR TO MONTH |

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

    # A GROUP BY key over an inline-table column stays non-nullable in Spark; Sail widens it.
    @sail-bug
    @function(nullability)
    Scenario: try_avg doctest #13 (schema)
      When query
        """
        SELECT g, try_avg(x) AS avg_x FROM VALUES ('bad', CAST(9223372036854775807 AS BIGINT)), ('bad', CAST(1 AS BIGINT)), ('ok', CAST(10 AS BIGINT)), ('ok', CAST(NULL AS BIGINT)), ('ok', CAST(5 AS BIGINT)) AS t(g, x) GROUP BY g ORDER BY g
        """
      Then query schema
        """
        root
         |-- g: string (nullable = false)
         |-- avg_x: double (nullable = true)
        """
