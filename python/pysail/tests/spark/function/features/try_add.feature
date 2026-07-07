@try_add
Feature: try_add with float and decimal inputs

  # try_add is ANSI-invariant: integer/decimal overflow returns NULL per element
  # regardless of spark.sql.ansi.enabled. Float add follows IEEE 754 (no
  # overflow-to-NULL). Decimal keeps a DECIMAL result type, never double.

  Rule: Float and double add keep their float type
    Scenario: double plus double
      When query
        """
        SELECT try_add(CAST(1.5 AS DOUBLE), CAST(2.5 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | 4.0    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: float plus float stays float
      When query
        """
        SELECT try_add(CAST(1.5 AS FLOAT), CAST(2.5 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 4.0    |
      Then query schema
        """
        root
         |-- result: float (nullable = true)
        """

  Rule: Decimal add keeps a DECIMAL result type
    Scenario: decimal plus decimal stays decimal
      When query
        """
        SELECT try_add(CAST(1.50 AS DECIMAL(10,2)), CAST(2.50 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 4.00   |
      Then query schema
        """
        root
         |-- result: decimal(11,2) (nullable = true)
        """

    Scenario: integer plus decimal stays decimal
      When query
        """
        SELECT try_add(2, CAST(2.5 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 4.50   |

  Rule: Overflow returns NULL and is ANSI-invariant
    Scenario: integer add overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_add(CAST(2147483647 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: integer add overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_add(CAST(2147483647 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: decimal add precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_add(CAST(9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: decimal add precision overflow returns NULL (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT try_add(CAST(9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL propagates
    Scenario: NULL operand yields NULL
      When query
        """
        SELECT try_add(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite doubles pass through, never NULL
    Scenario: infinity plus infinity is infinity
      When query
        """
        SELECT try_add(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: infinity plus negative infinity is NaN
      When query
        """
        SELECT try_add(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: NaN plus one is NaN
      When query
        """
        SELECT try_add(CAST('NaN' AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

  # Migrated from the former test_try_add.txt doctest (integer/date/interval/
  # timestamp add), so each function keeps a single feature-file source of truth.
  Rule: Integer add over columns, overflow to NULL
    Scenario: integer columns add with overflow rows nulled
      When query
        """
        SELECT try_add(CAST(birth AS INT), CAST(age AS INT)) AS result
        FROM VALUES (1982, 15), (1990, 2), (NULL, 10), (2147483647, 1), (-2147483648, -1) AS t(birth, age)
        """
      Then query result
        | result |
        | 1997   |
        | 1992   |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Date plus integer days
    Scenario: date plus integer days
      When query
        """
        SELECT
          try_add(DATE '2015-09-30', 1) AS d1,
          try_add(DATE '2000-01-01', 366) AS d2,
          try_add(DATE '2021-01-01', 1) AS d3,
          try_add(CAST(NULL AS DATE), 100) AS d4
        """
      Then query result
        | d1         | d2         | d3         | d4   |
        | 2015-10-01 | 2001-01-01 | 2021-01-02 | NULL |

  Rule: Date plus month interval clamps to month end
    Scenario: date plus positive month interval
      When query
        """
        SELECT
          try_add(DATE '2015-01-31', INTERVAL 1 MONTH) AS d1,
          try_add(DATE '2020-02-29', INTERVAL 12 MONTH) AS d2,
          try_add(CAST(NULL AS DATE), INTERVAL 3 MONTH) AS d3
        """
      Then query result
        | d1         | d2         | d3   |
        | 2015-02-28 | 2021-02-28 | NULL |

    Scenario: date plus negative month interval
      When query
        """
        SELECT
          try_add(DATE '2000-07-31', INTERVAL -1 MONTH) AS d1,
          try_add(DATE '2021-01-31', INTERVAL -1 MONTH) AS d2
        """
      Then query result
        | d1         | d2         |
        | 2000-06-30 | 2020-12-31 |

  Rule: Timestamp plus day interval
    Scenario: timestamp plus one day in UTC
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT try_add(TIMESTAMP '2021-01-01 00:00:00', INTERVAL 1 DAY) AS result
        """
      Then query result
        | result              |
        | 2021-01-02 00:00:00 |
