Feature: arithmetic result types (+ - * / %) vs Spark 4.2.0

  # `arithmetic_operand_resolution.feature` asserts that a pair RESOLVES; this pins WHAT TYPE it
  # resolves to, which that file declares out of its scope. Measured 2026-09-12 against Spark
  # 4.2.0 over the 1797 rows of that matrix: 1177 return the same type, 225 differ in 33 shapes,
  # and the remaining 395 are the pairs Sail rejects outright, already pinned there. One scenario
  # per root cause rather than per spelling, so 13 rows stand for the 225. None is fixed here -- the
  # operand-rejection work does not touch the coercion contract -- so every one is `@sail-bug`.

  Rule: an interval keeps the field range it was declared with

    # Spark's `YearMonthIntervalType`/`DayTimeIntervalType` carry `start_field` and `end_field`
    # (`YearMonthIntervalType.scala:50-59`, `DayTimeIntervalType.scala:54-63`) and name the range
    # they were declared with. Sail's Arrow types cannot carry them, so every year-month interval
    # reads YEAR TO MONTH and every day-time one DAY TO SECOND. The metadata that would fix it
    # exists only on the `fix/interval` branch, so this is the largest family that cannot be
    # closed from here: 56 of the 225 cells.
    @sail-bug
    Scenario Outline: <case> keeps its field range
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case          | expression                              | type                 |
        | month + month | INTERVAL '2' MONTH + INTERVAL '1' MONTH | interval month       |
        | year + year   | INTERVAL '2' YEAR + INTERVAL '1' YEAR   | interval year        |
        | day + day     | INTERVAL '2' DAY + INTERVAL '1' DAY     | interval day         |
        | hour + hour   | INTERVAL '25' HOUR + INTERVAL '1' HOUR  | interval hour        |
        | day + hour    | INTERVAL '2' DAY + INTERVAL '25' HOUR   | interval day to hour |

  Rule: subtracting two datetimes yields an interval

    # `SubtractTimestamps` returns `DayTimeIntervalType()` -- the full DAY TO SECOND range, which
    # Sail can spell -- and it takes the pair whenever either side is a timestamp
    # (`BinaryArithmeticWithDatetimeResolver.scala:139-141`), DATE included.
    Scenario: a date minus a timestamp is an interval
      When query
        """
        SELECT typeof(DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00') AS result
        """
      Then query result
        | result                 |
        | interval day to second |

    # TODO: `SubtractTimestamps` measures the difference in local time (`DateTimeUtils.subtractTimestamps`),
    #  and Sail subtracts the instants, so a DST change inside the difference shifts it by an hour. The
    #  root is `TIMESTAMP - TIMESTAMP`, which diverges the same way; the DATE arms only inherit it.
    @sail-bug
    Scenario: a timestamp minus a date is measured in local time across a DST change
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          CAST(TIMESTAMP'2024-03-10 12:00:00' - DATE'2024-03-10' AS STRING) AS a,
          CAST(DATE'2024-03-11' - TIMESTAMP'2024-03-10 00:00:00' AS STRING) AS b
        """
      Then query result
        | a                                   | b                                   |
        | INTERVAL '0 12:00:00' DAY TO SECOND | INTERVAL '1 00:00:00' DAY TO SECOND |

    # `SubtractDates` returns `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3617`). Sail
    # answers an INT day count: an Arrow `Duration` cannot carry the FIELD RANGE, and without it a
    # `Duration` is read by seconds wherever the difference is consumed (`CAST(... AS INT)` answered
    # 1209600). That needs the `SAIL::spark::interval` metadata, which lands with `fix/interval`.
    @sail-bug
    Scenario: a date minus a date is an interval with Spark's field range
      When query
        """
        SELECT typeof(DATE'2024-01-15' - DATE'2024-01-01') AS result
        """
      Then query result
        | result       |
        | interval day |

  Rule: a date shifted by a day-time interval becomes a timestamp

    # `BinaryArithmeticWithDatetimeResolver.scala:69` rewrites `date + <day-time>` into
    # `TimestampAddInterval`, so the result is a TIMESTAMP even though the left operand is a DATE.
    # Sail keeps the DATE, which silently drops the time-of-day part of the interval.
    @sail-bug
    Scenario: a date plus a day-time interval is a timestamp
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS result
        """
      Then query result
        | result    |
        | timestamp |

  Rule: numeric coercion matches Spark's

    # Spark's `findTightestCommonType`/decimal promotion, which Sail's coercion does not reproduce
    # for these three shapes. Together they are 84 of the 225 cells.
    @sail-bug
    Scenario Outline: numeric coercion: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case                              | expression                                 | type           |
        | a float with a decimal is double  | CAST(2 AS FLOAT) + CAST(2 AS DECIMAL(10,2)) | double         |
        | a modulo keeps the smallint width | CAST(2 AS SMALLINT) % CAST(2 AS SMALLINT)   | smallint       |
        | a modulo keeps the tinyint width  | CAST(2 AS TINYINT) % CAST(2 AS TINYINT)     | tinyint        |
        | a decimal division widens         | CAST(2 AS DECIMAL(10,2)) / CAST(2 AS INT)   | decimal(21,13) |

  Rule: a calendar interval divided by a number stays an interval

    # `BinaryArithmeticWithDatetimeResolver.scala:158` rewrites it into `DivideInterval`. Sail
    # used to coerce both sides to DOUBLE instead, losing the interval entirely -- and answering,
    # so no rejection test could see it. `arithmetic_calendar_interval_scaling.feature` now pins
    # the whole operation, value by value.
    Scenario: a calendar interval divided by a number is an interval
      When query
        """
        SELECT typeof(make_interval(0, 1, 0, 1, 0, 0, 0) / 2) AS result
        """
      Then query result
        | result   |
        | interval |
