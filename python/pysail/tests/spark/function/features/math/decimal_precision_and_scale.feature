# Tagged `@spark-4` because the `stored` lens reads the value off Arrow with `DataFrame.toArrow()`,
# which the PySpark client only offers from 4.0 on.
@spark-4
Feature: the precision and the scale Spark gives a decimal

  # Spark declares MAX_PRECISION = 38, MAX_SCALE = 38 and MINIMUM_ADJUSTED_SCALE = 6
  # (`DecimalType.scala:116-121`). When an arithmetic result would need more than 38 digits,
  # `adjustPrecisionScale` (`DecimalType.scala:175-197`) cuts the SCALE to make room for the
  # integral part, never below `min(scale, 6)`, and `spark.sql.decimalOperations.allowPrecisionLoss`
  # turns that trade off. A scale outside `0 <= scale <= precision` is refused, and a negative
  # scale cannot even be written in SQL: the parser rejects the `-`, whatever
  # `spark.sql.legacy.allowNegativeScaleOfDecimal` says -- that flag only reaches the DataType API.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: the corners of DECIMAL(precision, scale)

    Scenario Outline: a decimal at the corner: <case>
      When query template
        """
        <query>
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                                         | query                                   | stored                                   | type           | shown                                    | cast                                     |
        | the maximum precision with the minimum scale | SELECT CAST(1 AS DECIMAL(38,0)) AS v    | 1                                        | decimal(38,0)  | 1                                        | 1                                        |
        | the maximum precision with the maximum scale | SELECT CAST(0.1 AS DECIMAL(38,38)) AS v | 0.10000000000000000000000000000000000000 | decimal(38,38) | 0.10000000000000000000000000000000000000 | 0.10000000000000000000000000000000000000 |
        | the minimum precision with the minimum scale | SELECT CAST(1 AS DECIMAL(1,0)) AS v     | 1                                        | decimal(1,0)   | 1                                        | 1                                        |
        | the minimum precision with the maximum scale | SELECT CAST(0.1 AS DECIMAL(1,1)) AS v   | 0.1                                      | decimal(1,1)   | 0.1                                      | 0.1                                      |
        | a bare decimal literal                       | SELECT 1.5 AS v                         | 1.5                                      | decimal(2,1)   | 1.5                                      | 1.5                                      |

  Rule: the result type of decimal arithmetic

    Scenario Outline: arithmetic that needs no adjustment: <case>
      When query template
        """
        <query>
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                               | query                                                           | stored | type          | shown  | cast   |
        | a product whose scale already fits | SELECT CAST(1 AS DECIMAL(38,2)) * CAST(1 AS DECIMAL(38,2)) AS v | 1.0000 | decimal(38,4) | 1.0000 | 1.0000 |
        | a sum of the widest decimals       | SELECT CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0)) AS v | 2      | decimal(38,0) | 2      | 2      |

    @sail-bug
    Scenario Outline: arithmetic whose scale Spark cuts to make room: <case>
      When query template
        """
        <query>
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                                        | query                                                                 | stored   | type          | shown    | cast     |
        | a product whose scale is cut to the minimum | SELECT CAST(1 AS DECIMAL(38,10)) * CAST(1 AS DECIMAL(38,10)) AS v     | 1.000000 | decimal(38,6) | 1.000000 | 1.000000 |
        | a division of the widest scales             | SELECT CAST(0.1 AS DECIMAL(38,38)) / CAST(0.1 AS DECIMAL(38,38)) AS v | 1.000000 | decimal(38,6) | 1.000000 | 1.000000 |

  Rule: allowPrecisionLoss decides how much scale survives

    @sail-bug
    Scenario: a division with allowPrecisionLoss off
      Given config spark.sql.decimalOperations.allowPrecisionLoss = false
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,10)) / CAST(3 AS DECIMAL(38,10)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 0.333333333333333333 | decimal(38,18) | 0.333333333333333333 | 0.333333333333333333 |

    @sail-bug
    Scenario: a division with allowPrecisionLoss on
      Given config spark.sql.decimalOperations.allowPrecisionLoss = true
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,10)) / CAST(3 AS DECIMAL(38,10)) AS v
        """
      Then stored and printed result
        | stored | type | shown | cast |
        | 0.333333 | decimal(38,6) | 0.333333 | 0.333333 |

  Rule: the result type of a decimal aggregate

    Scenario Outline: an aggregate over a decimal column: <case>
      When query template
        """
        <query>
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                     | query                                                                                                        | stored   | type          | shown    | cast     |
        | a sum over a column      | SELECT SUM(v) AS v FROM (SELECT CAST(1.5 AS DECIMAL(10,2)) AS v UNION ALL SELECT CAST(2.5 AS DECIMAL(10,2))) | 4.00     | decimal(20,2) | 4.00     | 4.00     |
        | an average over a column | SELECT AVG(v) AS v FROM (SELECT CAST(1.5 AS DECIMAL(10,2)) AS v UNION ALL SELECT CAST(2.5 AS DECIMAL(10,2))) | 2.000000 | decimal(14,6) | 2.000000 | 2.000000 |

  Rule: a value that does not fit the type

    @sail-bug
    Scenario Outline: a value too large for the type: <case>
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        <query>
        """
      Then stored and printed result
        | stored   | type   | shown   | cast   |
        | <stored> | <type> | <shown> | <cast> |

      Examples:
        | case                                     | query                                  | stored | type         | shown | cast |
        | a value too large for the type, ANSI off | SELECT CAST(1000 AS DECIMAL(3,0)) AS v | NULL   | decimal(3,0) | NULL  | NULL |

    @sail-bug
    Scenario: a value too large for the type is rejected under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(1000 AS DECIMAL(3,0)) AS v
        """
      Then query error \[NUMERIC_VALUE_OUT_OF_RANGE\.WITH_SUGGESTION\]

  Rule: a type outside the declared range is rejected

    # TODO(parity): a precision past 38 is left untested on purpose. Spark refuses it with
    # DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION, while Sail ACCEPTS it and publishes the type -- its
    # ceiling is Arrow's Decimal256, so DECIMAL(39,0) through DECIMAL(76,0) all build and
    # DECIMAL(77,0) is the first one refused ("Max precision of a Decimal256 is 76"). It is a
    # contained superset, not a leak: no ordinary operation walks into it. Measured over
    # DECIMAL(38,0) -- unary minus, abs, +, *, /, %, SUM, AVG, round(-2), ceil, coalesce and
    # greatest -- every result type stays at precision 38, so only an explicit CAST reaches 39+.
    # Decide first whether Sail keeps the wider range; the test follows that call.

    @sail-bug
    Scenario: a scale greater than the precision
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,39)) AS v
        """
      Then query error Decimal scale \(39\) cannot be greater than precision \(38\)

    @sail-bug
    Scenario: a negative scale
      When query
        """
        SELECT CAST(1 AS DECIMAL(10,-2)) AS v
        """
      Then query error \[PARSE_SYNTAX_ERROR\] Syntax error at or near '-'

    # The legacy flag opens negative scales to the DataType API, never to the SQL syntax.
    @sail-bug
    Scenario: a negative scale with the legacy flag on
      Given config spark.sql.legacy.allowNegativeScaleOfDecimal = true
      When query
        """
        SELECT CAST(1 AS DECIMAL(10,-2)) AS v
        """
      Then query error \[PARSE_SYNTAX_ERROR\] Syntax error at or near '-'

    @sail-bug
    Scenario: a literal with more than 38 digits
      When query
        """
        SELECT 123456789012345678901234567890123456789.0 AS v
        """
      Then query error \[DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION\] Decimal precision 40 exceeds max precision 38
