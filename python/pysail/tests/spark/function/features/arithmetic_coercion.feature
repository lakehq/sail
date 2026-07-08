@arithmetic_coercion
Feature: Spark type coercion for the +, -, * operators

  # These are the coercion rules DataFusion's BinaryTypeCoercer does not perform
  # and Sail applies in the arithmetic plan builders to match Spark 4.1.1.

  Rule: Decimal with an integer literal narrows the literal to minimal precision
    Scenario: decimal times integer literal
      When query
        """
        SELECT CAST(2.5 AS DECIMAL(10,2)) * 3 AS result
        """
      Then query result
        | result |
        | 7.50   |
      Then query schema
        """
        root
         |-- result: decimal(12,2) (nullable = true)
        """

    Scenario: decimal plus integer literal
      When query
        """
        SELECT CAST(2.5 AS DECIMAL(10,2)) + 3 AS result
        """
      Then query result
        | result |
        | 5.50   |
      Then query schema
        """
        root
         |-- result: decimal(11,2) (nullable = true)
        """

    Scenario: decimal minus integer literal
      When query
        """
        SELECT CAST(5.5 AS DECIMAL(10,2)) - 2 AS result
        """
      Then query result
        | result |
        | 3.50   |
      Then query schema
        """
        root
         |-- result: decimal(11,2) (nullable = true)
        """

  Rule: Float times decimal promotes to double
    Scenario: float times decimal
      When query
        """
        SELECT CAST(1.5 AS FLOAT) * CAST(2.0 AS DECIMAL(10,2)) AS result
        """
      Then query result
        | result |
        | 3.0    |
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

  Rule: String operand coerces, ANSI-aware
    Scenario: string plus integer, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT '5' + 3 AS result
        """
      Then query result
        | result |
        | 8.0    |
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: string plus integer, ANSI on, casts string to the numeric operand
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT '5' + 3 AS result
        """
      Then query result
        | result |
        | 8      |

  Rule: Interval divided by an integer stays an interval
    # The regular `/` operator routes interval operands through SparkDivide (the
    # same unified UDF as try_divide), scaling the interval instead of erroring.
    Scenario: year-month interval divided by integer
      When query
        """
        SELECT make_ym_interval(1, 6) / 2 AS result
        """
      Then query result
        | result                       |
        | INTERVAL '0-9' YEAR TO MONTH |

    Scenario: day-time interval divided by integer
      When query
        """
        SELECT make_interval(0, 0, 0, 1, 0, 0, 0) / 2 AS result
        """
      Then query result
        | result   |
        | 12 hours |

  Rule: DATE minus DATE yields a day-time interval
    # Sail returns a day-time interval (Duration µs); Spark renders it as
    # INTERVAL '4' DAY (DAY-only field) while Sail renders DAY TO SECOND. The
    # value (4 days) is correct; the render granularity is the remaining gap.
    @sail-bug
    Scenario: date minus date is an interval
      When query
        """
        SELECT DATE '2020-01-05' - DATE '2020-01-01' AS result
        """
      Then query result
        | result            |
        | INTERVAL '4' DAY  |
