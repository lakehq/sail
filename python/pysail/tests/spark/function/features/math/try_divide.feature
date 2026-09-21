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

  Rule: Float and double divide, promoting the result to DOUBLE
    @sail-bug
    Scenario: double divided by double (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(CAST(10.0 AS DOUBLE), CAST(4.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 2.5    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    @sail-bug
    Scenario: double divided by double (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(CAST(10.0 AS DOUBLE), CAST(4.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 2.5    |

    @sail-bug
    Scenario: float divided by float promotes to double
      When query
        """
        SELECT try_divide(CAST(10.0 AS FLOAT), CAST(4.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 2.5    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Division by zero returns NULL and is ANSI-invariant
    @sail-bug
    Scenario: double divided by zero returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: double divided by zero returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: decimal divided by zero returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(CAST(1.0 AS DECIMAL(10,2)), CAST(0.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: decimal divided by zero returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(CAST(1.0 AS DECIMAL(10,2)), CAST(0.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite doubles pass through, only checked exceptions become NULL
    @sail-bug
    Scenario: infinity divided by infinity is NaN
      When query
        """
        SELECT try_divide(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: infinity divided by two is infinity
      When query
        """
        SELECT try_divide(CAST('Infinity' AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    @sail-bug
    Scenario: NULL propagates
      When query
        """
        SELECT try_divide(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Decimal division keeps a DECIMAL result type with Spark's precision/scale
    # Decimal division follows Spark's rule s = max(6, s1 + p2 + 1) via
    # `decimal_divide` (HALF_UP), not Arrow's `div` scale.
    @sail-bug
    Scenario: decimal divided by decimal stays decimal
      When query
        """
        SELECT try_divide(CAST(10.0 AS DECIMAL(10,2)), CAST(4.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result         |
        | 2.5000000000000 |
      Then query schema
        """
        root
         |-- result: decimal(23,13) (nullable = true)
        """

    @sail-bug
    Scenario: double divided by decimal promotes to double
      When query
        """
        SELECT try_divide(CAST(10.0 AS DOUBLE), CAST(4.0 AS DECIMAL(10,2))) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  # Migrated from the former test_try_divide.txt doctest (integer and interval
  # division), so each function keeps a single feature-file source of truth.
  Rule: Integer division promotes to double
    Scenario: integer columns divide to double
      When query
        """
        SELECT try_divide(a, b) AS result
        FROM VALUES (6000, 15), (1990, 2) AS t(a, b)
        """
      Then query result
        | result |
        | 400.0  |
        | 995.0  |

  Rule: Day-time interval divided by an integer
    Scenario: one day divided by two is twelve hours
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result   |
        | 12 hours |

    Scenario: ten hours divided by four
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 0, 10, 0, 0), 4) AS result
        """
      Then query result
        | result             |
        | 2 hours 30 minutes |

    Scenario: three days twelve hours divided by three
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 3, 12, 0, 0), 3) AS result
        """
      Then query result
        | result        |
        | 1 days 4 hours |

    Scenario: one week divided by two
      When query
        """
        SELECT try_divide(make_interval(0, 0, 1, 0, 0, 0, 0), 2) AS result
        """
      Then query result
        | result         |
        | 3 days 12 hours |

    Scenario: negative one day divided by two
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, -1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result    |
        | -12 hours |

    Scenario: interval divided by NULL is NULL
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: interval divided by zero is NULL
      When query
        """
        SELECT try_divide(make_interval(0, 0, 0, 1, 0, 0, 0), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Year-month interval divided by an integer
    Scenario: one year six months divided by two
      When query
        """
        SELECT try_divide(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-9' YEAR TO MONTH |
