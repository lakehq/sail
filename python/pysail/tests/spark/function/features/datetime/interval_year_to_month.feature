Feature: INTERVAL YEAR TO MONTH operations

  Rule: Arithmetic

    # Results are cast to STRING because a year-month interval cannot cross the Spark Connect
    # Arrow boundary: returning one directly fails on both engines with
    # UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION.

    Scenario: adding two year-month intervals
      When query
        """
        SELECT CAST(INTERVAL '1' YEAR + INTERVAL '2' MONTH AS STRING) AS result
        """
      Then query result
        | result                       |
        | INTERVAL '1-2' YEAR TO MONTH |

    # Sail fails to plan these: "Cannot get result type for temporal operation
    # Interval(YearMonth) * Int32". The day-time equivalents work, and so does
    # try_multiply on a year-month interval, so the gap is specific to the
    # `*` and `/` operators on Interval(YearMonth).
    @sail-bug
    Scenario Outline: multiplying and dividing a year-month interval: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        FROM VALUES (2) AS t(y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | expr                        | result                       |
        | interval * column  | INTERVAL '1' YEAR * y       | INTERVAL '2-0' YEAR TO MONTH |
        | column * interval  | y * INTERVAL '1' YEAR       | INTERVAL '2-0' YEAR TO MONTH |
        | interval * literal | make_ym_interval(1, 6) * 2  | INTERVAL '3-0' YEAR TO MONTH |
        | interval / column  | INTERVAL '2' YEAR / y       | INTERVAL '1-0' YEAR TO MONTH |

  Rule: Literal bounds

    Scenario: the largest YEAR TO MONTH literal parses
      When query
        """
        SELECT CAST(INTERVAL '178956970-7' YEAR TO MONTH AS STRING) AS result
        """
      Then query result
        | result                               |
        | INTERVAL '178956970-7' YEAR TO MONTH |

    @sail-bug
    Scenario: the most negative YEAR TO MONTH literal parses
      When query
        """
        SELECT CAST(INTERVAL '-178956970-8' YEAR TO MONTH AS STRING) AS result
        """
      Then query result
        | result                                |
        | INTERVAL '-178956970-8' YEAR TO MONTH |

    @sail-bug
    Scenario: a YEAR TO MONTH literal one month past the maximum is rejected
      When query
        """
        SELECT CAST(INTERVAL '178956970-8' YEAR TO MONTH AS STRING) AS result
        """
      Then query error \[INVALID_INTERVAL_FORMAT\.INTERVAL_PARSING\]

  # Spark 4.2.0 intervalExpressions.scala `DivideYMInterval` / `MultiplyYMInterval`: the
  # month result rounds HALF_UP (IntMath.divide, DoubleMath.roundToInt,
  # BigDecimal.setScale(0, HALF_UP)); multiplyExact raises "integer overflow" and
  # divideByZeroCheck raises INTERVAL_DIVIDED_BY_ZERO, neither ANSI-gated.
  Rule: Year-month interval times or divided by a number

    @sail-bug
    Scenario Outline: year-month interval rounding: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | expr                                     | result                        |
        | one month divided by two  | INTERVAL '1' MONTH / 2                   | INTERVAL '0-1' YEAR TO MONTH  |
        | minus one month by two    | INTERVAL '-1' MONTH / 2                  | INTERVAL '-0-1' YEAR TO MONTH |
        | three months by two       | INTERVAL '3' MONTH / 2                   | INTERVAL '0-2' YEAR TO MONTH  |
        | divided by decimal        | INTERVAL '1' MONTH / 2.0                 | INTERVAL '0-1' YEAR TO MONTH  |
        | times decimal             | INTERVAL '1' MONTH * 1.5                 | INTERVAL '0-2' YEAR TO MONTH  |
        | times double              | INTERVAL '1' MONTH * CAST(2.5 AS DOUBLE) | INTERVAL '0-3' YEAR TO MONTH  |

    @sail-bug
    Scenario: year-month interval rounding per row over columns
      When query
        """
        SELECT CAST(i / n AS STRING) AS q, CAST(i * f AS STRING) AS p
        FROM VALUES
          (make_ym_interval(0, 1), 2, CAST(1.5 AS DOUBLE)),
          (make_ym_interval(0, -1), 2, CAST(2.5 AS DOUBLE)),
          (make_ym_interval(0, 3), 2, CAST(0.5 AS DOUBLE))
        AS t(i, n, f)
        """
      Then query result
        | q                             | p                             |
        | INTERVAL '0-1' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |
        | INTERVAL '-0-1' YEAR TO MONTH | INTERVAL '-0-3' YEAR TO MONTH |
        | INTERVAL '0-2' YEAR TO MONTH  | INTERVAL '0-2' YEAR TO MONTH  |

    @sail-bug
    Scenario Outline: year-month interval divided by zero: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(i / <divisor> AS STRING) AS result
        FROM VALUES (make_ym_interval(1, 0), 1), (make_ym_interval(2, 0), 0) AS t(i, n)
        """
      Then query error \[INTERVAL_DIVIDED_BY_ZERO\]

      Examples:
        | case                  | ansi  | divisor |
        | literal, ANSI on      | true  | 0       |
        | literal, ANSI off     | false | 0       |
        | column, ANSI on       | true  | n       |
        | column, ANSI off      | false | n       |

    @sail-bug
    Scenario Outline: year-month interval multiplication overflow: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(i * 2 AS STRING) AS result
        FROM VALUES (make_ym_interval(1, 0)), (make_ym_interval(178956970, 0)) AS t(i)
        """
      Then query error integer overflow

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    Scenario Outline: try_ variants return NULL on year-month overflow: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case         | expr                                                  |
        | try_multiply | try_multiply(INTERVAL '178956970' YEAR, 2)            |
        | try_add      | try_add(make_ym_interval(178956970, 7), INTERVAL '1' MONTH) |
        | try_divide   | try_divide(INTERVAL '1' YEAR, 0)                      |

  # Adding and negating year-month intervals use Math.*Exact regardless of ANSI
  # (Spark 4.2.0 arithmetic.scala); abs raises ARITHMETIC_OVERFLOW.
  Rule: Year-month interval addition, negation and abs overflow

    @sail-bug
    Scenario Outline: year-month interval addition overflow: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(INTERVAL '178956970-7' YEAR TO MONTH + INTERVAL '1' MONTH AS STRING) AS result
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITH_SUGGESTION\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario Outline: negating the minimum year-month interval overflows: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(-i AS STRING) AS result
        FROM VALUES (make_ym_interval(1, 0)), (make_ym_interval(-178956970, -8)) AS t(i)
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITHOUT_SUGGESTION\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    Scenario Outline: abs of the minimum year-month interval overflows: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(abs(make_ym_interval(-178956970, -8)) AS STRING) AS result
        """
      Then query error \[ARITHMETIC_OVERFLOW\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

  # Spark 4.2.0 Cast.scala: a year-month interval casts to an integral in units of its END
  # field (IntervalUtils.yearMonthIntervalToInt), and a number casts to a year-month interval
  # in units of the target's end field (intToYearMonthInterval, CAST_OVERFLOW in both ANSI
  # modes). A STRING parses with castStringToYMInterval.
  Rule: Casts between year-month intervals and numbers or strings

    @sail-bug
    Scenario Outline: a year-month interval casts to an integral: <case>
      When query
        """
        SELECT CAST(<expr> AS <type>) AS result FROM VALUES (make_ym_interval(1, 2)), (make_ym_interval(-3, 0)) AS t(i)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                     | expr                            | type   | r1  | r2  |
        | column to INT in months  | i                               | INT    | 14  | -36 |
        | literal to BIGINT        | INTERVAL '-1-2' YEAR TO MONTH   | BIGINT | -14 | -14 |
        | year literal to INT      | INTERVAL '3' YEAR               | INT    | 3   | 3   |

    Scenario: a number casts to YEAR TO MONTH as months
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL YEAR TO MONTH) AS STRING) AS result FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                        |
        | INTERVAL '0-2' YEAR TO MONTH  |
        | INTERVAL '-0-3' YEAR TO MONTH |

    @sail-bug
    Scenario: a number casts to INTERVAL YEAR as years
      When query
        """
        SELECT CAST(CAST(CAST(n AS INTERVAL YEAR) AS INTERVAL YEAR TO MONTH) AS STRING) AS result
        FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                        |
        | INTERVAL '2-0' YEAR TO MONTH  |
        | INTERVAL '-3-0' YEAR TO MONTH |

    @sail-bug
    Scenario Outline: a number that does not fit INTERVAL YEAR: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL YEAR) AS STRING) AS result FROM VALUES (1), (200000000) AS t(n)
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario: a string casts to a year-month interval
      When query
        """
        SELECT CAST(CAST(s AS INTERVAL YEAR TO MONTH) AS STRING) AS result FROM VALUES ('1-2'), ('-3-4') AS t(s)
        """
      Then query result
        | result                        |
        | INTERVAL '1-2' YEAR TO MONTH  |
        | INTERVAL '-3-4' YEAR TO MONTH |
