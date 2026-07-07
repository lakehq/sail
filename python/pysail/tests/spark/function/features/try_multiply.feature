@try_multiply
Feature: try_multiply with float and decimal inputs

  # try_multiply is ANSI-invariant: integer/decimal overflow returns NULL per
  # element regardless of spark.sql.ansi.enabled. Float multiply follows IEEE 754
  # (no overflow-to-NULL). Decimal keeps a DECIMAL result type, never double.

  Rule: Float and double multiply keep their float type
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

    Scenario: decimal multiply precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_multiply(CAST(1e19 AS DECIMAL(38,0)), CAST(1e19 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

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
    Scenario: infinity times zero is NaN
      When query
        """
        SELECT try_multiply(CAST('Infinity' AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: infinity times two is infinity
      When query
        """
        SELECT try_multiply(CAST('Infinity' AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

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
