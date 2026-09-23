Feature: try_multiply output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_multiply stays nullable
      When query
        """
        SELECT try_multiply(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_multiply.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_multiply(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | args                                                  | result                       |
        | try_multiply doctest #1 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 2                 | 2 days                       |
        | try_multiply doctest #2 (result)  | make_interval(0, 0, 0, 0, 10, 0, 0), 4                | 40 hours                     |
        | try_multiply doctest #3 (result)  | make_interval(0, 0, 0, 3, 12, 0, 0), 3                | 9 days 36 hours              |
        | try_multiply doctest #4 (result)  | make_interval(0, 0, 0, 0, 0, 0, 10.5), 2              | 21 seconds                   |
        | try_multiply doctest #5 (result)  | make_interval(0, 0, 1, 0, 0, 0, 0), 2                 | 14 days                      |
        | try_multiply doctest #6 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT) | NULL                         |
        | try_multiply doctest #7 (result)  | make_interval(0, 0, 0, 1, 0, 0, 0), 0                 | 0 seconds                    |
        | try_multiply doctest #8 (result)  | make_interval(0, 0, 0, -1, 0, 0, 0), 2                | -2 days                      |
        | try_multiply doctest #9 (result)  | make_interval(0, 0, 0, 1, 0, 90, 0), 2                | 2 days 3 hours               |
        | try_multiply doctest #10 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '3-0' YEAR TO MONTH |
        | try_multiply doctest #11 (result) | make_ym_interval(1, 6), 2                             | INTERVAL '3-0' YEAR TO MONTH |

  Rule: Float and double multiply keep their float type
    @sail-bug
    Scenario: double times double
      When query
        """
        SELECT try_multiply(CAST(2.5 AS DOUBLE), CAST(3.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 7.5    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    @sail-bug
    Scenario: float times float stays float
      When query
        """
        SELECT try_multiply(CAST(1.5 AS FLOAT), CAST(2.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 3.0    |
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Decimal multiply keeps a DECIMAL result type
    @sail-bug
    Scenario: decimal times decimal stays decimal
      When query
        """
        SELECT try_multiply(CAST(2.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 7.5000 |
      Then query schema
        """
        root
         |-- result: decimal(21,4) (nullable = true)
        """

    @sail-bug
    Scenario: integer times decimal stays decimal
      When query
        """
        SELECT try_multiply(2, CAST(2.5 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 5.00   |

  Rule: Overflow returns NULL and is ANSI-invariant
    Scenario: integer multiply overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_multiply(CAST(2147483647 AS INT), CAST(2 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: integer multiply overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_multiply(CAST(2147483647 AS INT), CAST(2 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: decimal multiply precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_multiply(CAST(1e19 AS DECIMAL(38,0)), CAST(1e19 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: decimal multiply precision overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_multiply(CAST(1e19 AS DECIMAL(38,0)), CAST(1e19 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite doubles pass through, never NULL
    @sail-bug
    Scenario: infinity times zero is NaN
      When query
        """
        SELECT try_multiply(CAST('Infinity' AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: infinity times two is infinity
      When query
        """
        SELECT try_multiply(CAST('Infinity' AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    @sail-bug
    Scenario: zero times negative infinity is NaN
      When query
        """
        SELECT try_multiply(CAST(0.0 AS DOUBLE), CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: Per-element overflow in an array nulls only the offending rows
    Scenario: mixed integer multiply array nulls only overflow rows
      When query
        """
        SELECT try_multiply(CAST(a AS INT), CAST(b AS INT)) AS result
        FROM VALUES (2147483647, 2), (3, 4), (1073741824, 4) AS t(a, b)
        """
      Then query result ordered
        | result |
        | NULL   |
        | 12     |
        | NULL   |

    @sail-bug
    Scenario: mixed decimal multiply array nulls only precision-overflow rows
      When query
        """
        SELECT try_multiply(CAST(a AS DECIMAL(38,0)), CAST(b AS DECIMAL(38,0))) AS result
        FROM VALUES (1e19, 1e19), (2, 3), (1e20, 1e20) AS t(a, b)
        """
      Then query result ordered
        | result |
        | NULL   |
        | 6      |
        | NULL   |

  Rule: NULL propagates
    @sail-bug
    Scenario: NULL operand yields NULL
      When query
        """
        SELECT try_multiply(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  # Migrated from the former test_try_multiply.txt doctest (interval multiply),
  # so each function keeps a single feature-file source of truth.
  Rule: Day-time interval multiplied by an integer
    Scenario: one day times two
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result |
        | 2 days |

    Scenario: ten hours times four
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 0, 10, 0, 0), 4) AS result
        """
      Then query result
        | result   |
        | 40 hours |

    Scenario: three days twelve hours times three
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 3, 12, 0, 0), 3) AS result
        """
      Then query result
        | result         |
        | 9 days 36 hours |

    Scenario: one week times two
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 1, 0, 0, 0, 0), 2) AS result
        """
      Then query result
        | result  |
        | 14 days |

    Scenario: negative one day times two
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, -1, 0, 0, 0), 2) AS result
        """
      Then query result
        | result  |
        | -2 days |

    Scenario: interval times NULL is NULL
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: interval times zero
      When query
        """
        SELECT try_multiply(make_interval(0, 0, 0, 1, 0, 0, 0), 0) AS result
        """
      Then query result
        | result    |
        | 0 seconds |

  Rule: Year-month interval multiplied by an integer
    Scenario: one year six months times two
      When query
        """
        SELECT try_multiply(make_ym_interval(1, 6), 2) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '3-0' YEAR TO MONTH |

  Rule: Day-time interval argument

    # The doctests above cover calendar and year-month intervals, which work. A DAY TO SECOND
    # interval (Arrow `Duration`) does not: Sail fails with "Function 'try_multiply' user-defined
    # coercion failed". Note the gap is the mirror image of the `*` operator, which handles
    # day-time intervals but not year-month ones.
    @sail-bug
    Scenario Outline: try_multiply on a DAY TO SECOND interval by a <case>
      When query
        """
        SELECT CAST(try_multiply(INTERVAL '1' DAY, <multiplier>) AS STRING) AS result
        FROM VALUES (2) AS t(y)
        """
      Then query result
        | result                              |
        | INTERVAL '2 00:00:00' DAY TO SECOND |

      Examples:
        | case    | multiplier |
        | literal | 2          |
        | column  | y          |
