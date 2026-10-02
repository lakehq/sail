Feature: try_add output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_add stays nullable
      When query
        """
        SELECT try_add(c, 2) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_try_add.txt doctests)

    Scenario: try_add doctest #1 (result)
      When query
        """
        SELECT birth, age, try_add(birth, age) AS birth_plus_age FROM VALUES (CAST(1982 AS INT), CAST(15 AS INT)), (CAST(1990 AS INT), CAST(2 AS INT)), (CAST(NULL AS INT), CAST(10 AS INT)), (CAST(2147483647 AS INT), CAST(1 AS INT)), (CAST(-2147483648 AS INT), CAST(-1 AS INT)) AS t(birth, age)
        """
      Then query result
        | birth       | age | birth_plus_age |
        | 1982        | 15  | 1997           |
        | 1990        | 2   | 1992           |
        | NULL        | 10  | NULL           |
        | 2147483647  | 1   | NULL           |
        | -2147483648 | -1  | NULL           |

    Scenario: try_add doctest #2 (result)
      When query
        """
        SELECT try_add(DATE '2015-09-30', 1) as d1, try_add(DATE '2000-01-01', 366) as d2, try_add(DATE '2021-01-01', 1) as d3, try_add(NULL, 100) as d4
        """
      Then query result
        | d1         | d2         | d3         | d4   |
        | 2015-10-01 | 2001-01-01 | 2021-01-02 | NULL |

    Scenario: try_add doctest #3 (result)
      When query
        """
        SELECT try_add(DATE '2015-01-31', INTERVAL 1 MONTH) as d1, try_add(DATE '2020-02-29', INTERVAL 12 MONTH) as d2, try_add(NULL, INTERVAL 3 MONTH) as d3
        """
      Then query result
        | d1         | d2         | d3   |
        | 2015-02-28 | 2021-02-28 | NULL |

    Scenario: try_add doctest #4 (result)
      When query
        """
        SELECT try_add(DATE '2000-07-31', INTERVAL -1 MONTH) as d1, try_add(DATE '2021-01-31', INTERVAL -1 MONTH) as d2
        """
      Then query result
        | d1         | d2         |
        | 2000-06-30 | 2020-12-31 |

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_add(<args>) as result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | args                                            | result              |
        | try_add doctest #6 (result) | TIMESTAMP '2021-01-01 00:00:00', INTERVAL 1 DAY | 2021-01-02 00:00:00 |

    # Adding two YEAR intervals keeps the YEAR-only field range in Spark, which renders as
    # `INTERVAL '3' YEAR`. Sail widens the result to YEAR TO MONTH and renders `'3-0'`.
    @sail-bug
    Scenario: try_add doctest #5 (result)
      When query
        """
        SELECT try_add(INTERVAL '1' YEAR, INTERVAL '2' YEAR) as result
        """
      Then query result
        | result            |
        | INTERVAL '3' YEAR |

  Rule: Basic integer addition

    Scenario: INT plus INT
      When query
        """
        SELECT try_add(1, 2) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Negative plus negative
      When query
        """
        SELECT try_add(-10, -20) AS result
        """
      Then query result
        | result |
        | -30    |

    Scenario: Positive plus negative
      When query
        """
        SELECT try_add(10, -3) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: INT plus zero
      When query
        """
        SELECT try_add(42, 0) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Zero plus zero
      When query
        """
        SELECT try_add(0, 0) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Mixed INT and BIGINT coercion

    @sail-bug
    Scenario: INT plus BIGINT promotes to BIGINT
      When query
        """
        SELECT try_add(CAST(1 AS INT), CAST(2 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    Scenario: BIGINT plus INT promotes to BIGINT
      When query
        """
        SELECT try_add(CAST(100 AS BIGINT), CAST(200 AS INT)) AS result
        """
      Then query result
        | result |
        | 300    |

    Scenario: BIGINT plus BIGINT
      When query
        """
        SELECT try_add(CAST(1 AS BIGINT), CAST(2 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: Overflow returns NULL

    Scenario: INT positive overflow returns NULL
      When query
        """
        SELECT try_add(CAST(2147483647 AS INT), CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: INT negative overflow returns NULL
      When query
        """
        SELECT try_add(CAST(-2147483648 AS INT), CAST(-1 AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: BIGINT positive overflow returns NULL
      When query
        """
        SELECT try_add(CAST(9223372036854775807 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: BIGINT negative overflow returns NULL
      When query
        """
        SELECT try_add(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: BIGINT max minus one does not overflow
      When query
        """
        SELECT try_add(CAST(9223372036854775806 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

  Rule: Date addition

    Scenario: Date plus integer days
      When query
        """
        SELECT try_add(DATE '2024-01-01', 5) AS result
        """
      Then query result
        | result     |
        | 2024-01-06 |

    Scenario: Date plus negative integer subtracts days
      When query
        """
        SELECT try_add(DATE '2024-01-10', -5) AS result
        """
      Then query result
        | result     |
        | 2024-01-05 |

    Scenario: Date plus zero days
      When query
        """
        SELECT try_add(DATE '2024-06-15', 0) AS result
        """
      Then query result
        | result     |
        | 2024-06-15 |

    Scenario: Date plus year-month interval
      When query
        """
        SELECT try_add(DATE '2024-01-31', INTERVAL 1 MONTH) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: Date plus negative year-month interval
      When query
        """
        SELECT try_add(DATE '2024-03-31', INTERVAL -1 MONTH) AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

  Rule: NULL handling

    Scenario: NULL left operand returns NULL
      When query
        """
        SELECT try_add(CAST(NULL AS INT), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL right operand returns NULL
      When query
        """
        SELECT try_add(5, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Both operands NULL returns NULL
      When query
        """
        SELECT try_add(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL date plus integer returns NULL
      When query
        """
        SELECT try_add(CAST(NULL AS DATE), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Float and double add keep their float type
    @sail-bug
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

    @sail-bug
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
    @sail-bug
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

    @sail-bug
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

    @sail-bug
    Scenario: decimal add precision overflow returns NULL (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_add(CAST(9e37 AS DECIMAL(38,0)), CAST(9e37 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
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
    @sail-bug
    Scenario: NULL operand yields NULL
      When query
        """
        SELECT try_add(CAST(NULL AS DOUBLE), CAST(2.0 AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite doubles pass through, never NULL
    @sail-bug
    Scenario: infinity plus infinity is infinity
      When query
        """
        SELECT try_add(CAST('Infinity' AS DOUBLE), CAST('Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | Infinity |

    @sail-bug
    Scenario: infinity plus negative infinity is NaN
      When query
        """
        SELECT try_add(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
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
