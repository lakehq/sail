Feature: arithmetic result types (+ - * / %) vs Spark 4.2.0

  # `arithmetic_operand_resolution.feature` asserts that a pair RESOLVES; this pins WHAT TYPE it
  # resolves to, which that file declares out of its scope. Measured 2026-09-12 against Spark
  # 4.2.0 over the 1797 rows of that matrix: 1177 return the same type, 225 differ in 33 shapes,
  # and the remaining 395 are the pairs Sail rejects outright, already pinned there. One scenario
  # per root cause rather than per spelling, so 13 rows stand for the 225. None is fixed here -- the
  # TODO: each `@sail-bug` here is a RESULT TYPE, never an accept/reject, so it belongs to the
  #   arithmetic coercion work rather than to this PR; the interval field ranges among them need
  #   PR #2350.
  # operand-rejection work does not touch the coercion contract -- so every one is `@sail-bug`.

  Rule: an interval keeps the field range it was declared with

    # Spark's `YearMonthIntervalType`/`DayTimeIntervalType` carry `start_field` and `end_field`
    # (`YearMonthIntervalType.scala:50-59`, `DayTimeIntervalType.scala:54-63`) and name the range
    # they were declared with. Sail's Arrow types cannot carry them, so every year-month interval
    # reads YEAR TO MONTH and every day-time one DAY TO SECOND. The metadata that would fix it
    # arrives with PR #2350, so this is the largest family that cannot be
    # closed from here: 56 of the 225 cells.
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
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT typeof(DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00') AS result
        """
      Then query result
        | result                 |
        | interval day to second |

    # `SubtractTimestamps` measures the difference in local time (`DateTimeUtils.subtractTimestamps`),
    # rather than in UTC instants, so the two sides of a DST transition still differ by their
    # wall-clock readings.
    Scenario: a timestamp minus a date is measured in local time across a DST change
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          CAST(TIMESTAMP'2024-03-10 12:00:00' - DATE'2024-03-10' AS STRING) AS a,
          CAST(DATE'2024-03-11' - TIMESTAMP'2024-03-10 00:00:00' AS STRING) AS b,
          typeof(TIMESTAMP'2024-03-10 12:00:00' - DATE'2024-03-10') AS a_type,
          typeof(DATE'2024-03-11' - TIMESTAMP'2024-03-10 00:00:00') AS b_type
        """
      Then query result
        | a                                   | b                                   | a_type                 | b_type                 |
        | INTERVAL '0 12:00:00' DAY TO SECOND | INTERVAL '1 00:00:00' DAY TO SECOND | interval day to second | interval day to second |

    Scenario: two timestamps are measured in local time across a DST change
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          CAST(TIMESTAMP'2024-03-10 12:00:00' - TIMESTAMP'2024-03-10 00:00:00' AS STRING) AS result,
          typeof(TIMESTAMP'2024-03-10 12:00:00' - TIMESTAMP'2024-03-10 00:00:00') AS type
        """
      Then query result
        | result                              | type                    |
        | INTERVAL '0 12:00:00' DAY TO SECOND | interval day to second  |

    # `SubtractDates` returns `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3617`). Sail
    # answers an INT day count: an Arrow `Duration` cannot carry the FIELD RANGE, and without it a
    # `Duration` is read by seconds wherever the difference is consumed (`CAST(... AS INT)` answered
    # 1209600). That needs the `SAIL::spark::interval` metadata, which lands with PR #2350.
    Scenario: a date minus a date is an interval with Spark's field range
      Given config spark.sql.session.timeZone = UTC
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
    # Preserve the time-of-day part as well as the result type.
    Scenario: a date plus a day-time interval is a timestamp
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT typeof(DATE'2024-01-15' + INTERVAL '25' HOUR) AS result
        """
      Then query result
        | result    |
        | timestamp |

  Rule: numeric coercion matches Spark's

    # Spark's decimal precision rule converts a fixed decimal paired with a floating point value
    # to DOUBLE before each binary arithmetic operator (`DecimalPrecision.scala:45-47`).
    Scenario Outline: a decimal with a floating point value becomes double: <operator> (<order>, ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<left> <operator> <right>) AS result
        """
      Then query result
        | result |
        | double |

      Examples:
        | operator | order           | ansi  | left                       | right                      |
        | +        | float, decimal  | false | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | -        | float, decimal  | false | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | *        | float, decimal  | false | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | /        | float, decimal  | false | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | %        | float, decimal  | false | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | +        | float, decimal  | true  | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | -        | float, decimal  | true  | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | *        | float, decimal  | true  | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | /        | float, decimal  | true  | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | %        | float, decimal  | true  | CAST(2 AS FLOAT)           | CAST(2 AS DECIMAL(10,2))   |
        | +        | decimal, float  | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | -        | decimal, float  | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | *        | decimal, float  | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | /        | decimal, float  | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | %        | decimal, float  | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | +        | decimal, float  | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | -        | decimal, float  | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | *        | decimal, float  | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | /        | decimal, float  | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | %        | decimal, float  | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS FLOAT)           |
        | +        | double, decimal | false | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | -        | double, decimal | false | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | *        | double, decimal | false | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | /        | double, decimal | false | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | %        | double, decimal | false | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | +        | double, decimal | true  | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | -        | double, decimal | true  | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | *        | double, decimal | true  | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | /        | double, decimal | true  | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | %        | double, decimal | true  | CAST(2 AS DOUBLE)          | CAST(2 AS DECIMAL(10,2))   |
        | +        | decimal, double | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | -        | decimal, double | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | *        | decimal, double | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | /        | decimal, double | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | %        | decimal, double | false | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | +        | decimal, double | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | -        | decimal, double | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | *        | decimal, double | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | /        | decimal, double | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |
        | %        | decimal, double | true  | CAST(2 AS DECIMAL(10,2))   | CAST(2 AS DOUBLE)          |

    # Decimal precision and scale are operation-specific. This is deliberately separate from the
    # floating-point rule above, which turns the pair into DOUBLE before evaluation.
    Scenario Outline: a decimal division uses Spark's precision and scale: <case> (ANSI <ansi>)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<left> / <right>) AS result
        """
      Then query result
        | result         |
        | <type>         |

      # Spark `Divide.resultDecimalType` uses:
      # precision = p1 - s1 + s2 + max(6, s1 + p2 + 1), scale = max(6, s1 + p2 + 1).
      Examples:
        | case                | ansi  | left                      | right                     | type           |
        | decimal / tinyint   | false | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS TINYINT)        | decimal(14,6)  |
        | decimal / smallint  | false | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS SMALLINT)       | decimal(16,8)  |
        | decimal / int       | false | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS INT)            | decimal(21,13) |
        | decimal / bigint    | false | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS BIGINT)         | decimal(31,23) |
        | tinyint / decimal   | false | CAST(2 AS TINYINT)        | CAST(2 AS DECIMAL(10,2))  | decimal(16,11) |
        | smallint / decimal  | false | CAST(2 AS SMALLINT)       | CAST(2 AS DECIMAL(10,2))  | decimal(18,11) |
        | int / decimal       | false | CAST(2 AS INT)            | CAST(2 AS DECIMAL(10,2))  | decimal(23,11) |
        | bigint / decimal    | false | CAST(2 AS BIGINT)         | CAST(2 AS DECIMAL(10,2))  | decimal(33,11) |
        | decimal / decimal   | false | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS DECIMAL(5,1))   | decimal(17,8)  |
        | adjusted scale       | false | CAST(2 AS DECIMAL(30,15)) | CAST(2 AS DECIMAL(30,15)) | decimal(38,8)  |
        | adjusted scale floor | false | CAST(2 AS DECIMAL(38,0))  | CAST(2 AS DECIMAL(38,0))  | decimal(38,6)  |
        | decimal / tinyint   | true  | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS TINYINT)        | decimal(14,6)  |
        | decimal / smallint  | true  | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS SMALLINT)       | decimal(16,8)  |
        | decimal / int       | true  | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS INT)            | decimal(21,13) |
        | decimal / bigint    | true  | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS BIGINT)         | decimal(31,23) |
        | tinyint / decimal   | true  | CAST(2 AS TINYINT)        | CAST(2 AS DECIMAL(10,2))  | decimal(16,11) |
        | smallint / decimal  | true  | CAST(2 AS SMALLINT)       | CAST(2 AS DECIMAL(10,2))  | decimal(18,11) |
        | int / decimal       | true  | CAST(2 AS INT)            | CAST(2 AS DECIMAL(10,2))  | decimal(23,11) |
        | bigint / decimal    | true  | CAST(2 AS BIGINT)         | CAST(2 AS DECIMAL(10,2))  | decimal(33,11) |
        | decimal / decimal   | true  | CAST(2 AS DECIMAL(10,2))  | CAST(2 AS DECIMAL(5,1))   | decimal(17,8)  |
        | adjusted scale       | true  | CAST(2 AS DECIMAL(30,15)) | CAST(2 AS DECIMAL(30,15)) | decimal(38,8)  |
        | adjusted scale floor | true  | CAST(2 AS DECIMAL(38,0))  | CAST(2 AS DECIMAL(38,0))  | decimal(38,6)  |

    Scenario: decimal division rounds at Spark's public scale
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(CAST(2 AS INT) / CAST(3 AS DECIMAL(10,0)) AS STRING) AS value,
          typeof(CAST(2 AS INT) / CAST(3 AS DECIMAL(10,0))) AS type
        """
      Then query result
        | value         | type           |
        | 0.66666666667 | decimal(21,11) |

    Scenario Outline: a modulo keeps its narrow integral type: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS result
        """
      Then query result
        | result |
        | <type> |

      Examples:
        | case     | ansi  | expression                               | type     |
        | smallint | false | CAST(2 AS SMALLINT) % CAST(2 AS SMALLINT) | smallint |
        | smallint | true  | CAST(2 AS SMALLINT) % CAST(2 AS SMALLINT) | smallint |
        | tinyint  | false | CAST(2 AS TINYINT) % CAST(2 AS TINYINT)   | tinyint  |
        | tinyint  | true  | CAST(2 AS TINYINT) % CAST(2 AS TINYINT)   | tinyint  |

    @function(nullability)
    Scenario Outline: decimal and floating arithmetic keeps Spark's schema: <operator>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2 AS DECIMAL(10,2)) <operator> CAST(2 AS FLOAT) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = <nullable>)
        """

      # `BinaryArithmetic.nullable` follows the children for +, - and *, while `DivModLike`
      # overrides it to true for / and % because a zero divisor can yield NULL in legacy mode.
      Examples:
        | operator | nullable |
        | +        | false    |
        | -        | false    |
        | *        | false    |
        | /        | true     |
        | %        | true     |

    @function(nullability)
    Scenario: decimal division keeps Spark's precision, scale, and nullability
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(2 AS DECIMAL(10,2)) / CAST(2 AS INT) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(21,13) (nullable = true)
        """

    @function(nullability)
    Scenario Outline: datetime arithmetic keeps Spark's type and non-nullability: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <expression> AS result
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = false)
        """

      Examples:
        | case                         | expression                                                   | type                   |
        | date plus an hour             | DATE'2024-01-15' + INTERVAL '25' HOUR                       | timestamp              |
        | date minus timestamp          | DATE'2024-01-15' - TIMESTAMP'2024-01-01 00:00:00'           | interval day to second |
        | timestamp minus date          | TIMESTAMP'2024-01-15 12:00:00' - DATE'2024-01-01'           | interval day to second |
        | timestamp minus timestamp     | TIMESTAMP'2024-01-15 12:00:00' - TIMESTAMP'2024-01-01 00:00:00' | interval day to second |

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
