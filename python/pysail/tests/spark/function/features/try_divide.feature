@try_divide
Feature: try_divide with float and decimal inputs

  # try_divide is ANSI-invariant: division by zero and decimal precision overflow
  # return NULL regardless of spark.sql.ansi.enabled. Non-finite doubles (Inf/NaN)
  # are real values and pass through; only checked exceptions become NULL.

  Rule: Float and double divide, promoting the result to DOUBLE
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

    Scenario: double divided by double (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(CAST(10.0 AS DOUBLE), CAST(4.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 2.5    |

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
    Scenario: double divided by zero returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: double divided by zero returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_divide(CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: decimal divided by zero returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_divide(CAST(1.0 AS DECIMAL(10,2)), CAST(0.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | NULL   |

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
    Scenario: infinity divided by infinity is NaN
      When query
        """
        SELECT try_divide(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity divided by two is infinity
      When query
        """
        SELECT try_divide(CAST('Infinity' AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: NULL propagates
      When query
        """
        SELECT try_divide(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Decimal division keeps a DECIMAL result type, never double
    # Sail's decimal division precision/scale follows DataFusion/Arrow, not Spark's
    # rule (Spark: decimal(23,13) "2.5000000000000"; Sail: scale 6). Pre-existing
    # gap shared with the regular `/` operator (see the `TODO: Cast the precision
    # and scale that matches Spark` in spark_divide). The fix here is that decimal
    # division stays DECIMAL (not double); the exact scale is a separate follow-up.
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
