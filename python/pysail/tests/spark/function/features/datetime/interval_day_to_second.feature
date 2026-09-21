Feature: INTERVAL DAY TO SECOND literal parsing and operations

  Rule: Basic literals

    Scenario Outline: Literal: <case>
      When query
        """
        SELECT INTERVAL <lit> DAY TO SECOND AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | lit           | result                               |
        | negative interval      | '-3 04:05:06' | INTERVAL '-3 04:05:06' DAY TO SECOND |
        | negative zero interval | '-0 00:00:00' | INTERVAL '0 00:00:00' DAY TO SECOND  |

    Scenario: negative hours in interval is invalid
      When query
        """
        SELECT INTERVAL '3 -04:00:00' DAY TO SECOND AS result
        """
      Then query error (?i)invalid.*interval

    @sail-bug
    Scenario: Zero multi-unit intervals preserve their syntactic family
      When query
        """
        SELECT
          typeof(INTERVAL 0 YEAR 0 MONTH) AS year_month_type,
          typeof(INTERVAL 0 DAY 0 SECOND) AS day_time_type
        """
      Then query result
        | year_month_type        | day_time_type          |
        | interval year to month | interval day to second |

  Rule: Overflow and large values

    # Spark validates each field of a DAY TO SECOND literal and rejects an out-of-range hour
    # instead of carrying it into days: "requirement failed: hour 25 outside range [0, 23]".
    # Sail normalizes it to `INTERVAL '1 01:00:00' DAY TO SECOND`.
    @sail-bug
    Scenario: overflow hours into days
      When query
        """
        SELECT INTERVAL '0 25:00:00' DAY TO SECOND AS result
        """
      Then query error hour 25 outside range \[0, 23\]

  Rule: Cast operations

    Scenario Outline: Cast: <case>
      When query
        """
        SELECT CAST(<expr> AS INTERVAL DAY TO SECOND) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | expr                                                | result                              |
        | roundtrip cast to string and back    | CAST(INTERVAL '2 10:20:30' DAY TO SECOND AS STRING) | INTERVAL '2 10:20:30' DAY TO SECOND |
        | cast HOUR TO SECOND to DAY TO SECOND | INTERVAL '12:30:45' HOUR TO SECOND                  | INTERVAL '0 12:30:45' DAY TO SECOND |

  Rule: Arithmetic and comparison

    Scenario Outline: Arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | expr                                                                      | result                              |
        | addition of intervals | INTERVAL '0 23:00:00' DAY TO SECOND + INTERVAL '0 02:00:00' DAY TO SECOND | INTERVAL '1 01:00:00' DAY TO SECOND |

    Scenario Outline: string and day-time interval addition with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          ('2026-03-15 12:30:00'),
          ('not-a-timestamp'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | 2026-03-15 12:30:05 |
        | NULL                |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | operand order  | expr                                  |
        | string first   | ts_str + INTERVAL 1 SECOND * 5         |
        | interval first | INTERVAL 1 SECOND * 5 + ts_str         |

    Scenario: string and day-time interval addition under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: invalid string and day-time interval addition errors under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES ('not-a-timestamp') AS t(ts_str)
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|found n at 0:1)

    Scenario Outline: string and whole-day interval addition keeps local time across DST with <operand order>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2019-03-09 12:00:00'),
          ('2019-11-02 12:00:00')
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2019-03-10 12:00:00 |
        | 2019-11-03 12:00:00 |

      Examples:
        | operand order  | expr                     |
        | string first   | ts_str + INTERVAL 1 DAY  |
        | interval first | INTERVAL 1 DAY + ts_str  |

    Scenario Outline: leap-second string and interval addition returns NULL in legacy mode with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    Scenario Outline: leap-second string and interval addition errors in ANSI mode with <operand order>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|leap seconds)

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    # Same field validation as above: Spark will not even parse `INTERVAL '1 24:00:00'`, so the
    # normalized-equality comparison never runs. Sail normalizes and answers `true`.
    @sail-bug
    Scenario: equality with normalized form
      When query
        """
        SELECT INTERVAL '1 24:00:00' DAY TO SECOND = INTERVAL '2 00:00:00' DAY TO SECOND AS result
        """
      Then query error hour 24 outside range \[0, 23\]

    Scenario: comparison in WHERE clause
      When query
        """
        SELECT * FROM (VALUES (1)) t(a)
        WHERE INTERVAL '2 03:04:05' DAY TO SECOND > INTERVAL '1 23:59:59' DAY TO SECOND
        """
      Then query result
        | a |
        | 1 |

  Rule: Subquery and projection

    Scenario: interval in subquery
      When query
        """
        SELECT x FROM (SELECT INTERVAL '3 10:00:00' DAY TO SECOND AS x) t
        """
      Then query result
        | x                                   |
        | INTERVAL '3 10:00:00' DAY TO SECOND |

    @sail-bug
    Scenario: VALUES widens interval qualifiers across rows
      When query
        """
        SELECT day_time, year_month
        FROM VALUES
          (INTERVAL 1 DAY, INTERVAL 1 YEAR),
          (INTERVAL 1 HOUR, INTERVAL 1 MONTH),
          (NULL, NULL)
        AS t(day_time, year_month)
        """
      Then query schema
        """
        root
         |-- day_time: interval day to hour (nullable = true)
         |-- year_month: interval year to month (nullable = true)
        """

  Rule: Literal bounds

    @sail-bug
    Scenario: the most negative DAY TO SECOND literal parses
      When query
        """
        SELECT INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND AS result
        """
      Then query result
        | result                                              |
        | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |

    Scenario: the largest DAY TO SECOND literal parses
      When query
        """
        SELECT INTERVAL '106751991 04:00:54.775807' DAY TO SECOND AS result
        """
      Then query result
        | result                                             |
        | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND |

    @sail-bug
    Scenario: a DAY literal beyond Long microseconds overflows
      When query
        """
        SELECT INTERVAL '106751992' DAY AS result
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITHOUT_SUGGESTION\]

  # Spark 4.2.0 intervalExpressions.scala `DivideDTInterval` + trait `IntervalDivide`:
  # `divideByZeroCheck` always throws INTERVAL_DIVIDED_BY_ZERO; it is not ANSI-gated.
  # Sail says "divide by zero" under ANSI and returns NULL without it.
  Rule: Dividing a day-time interval by zero raises regardless of ANSI

    @sail-bug
    Scenario Outline: day-time interval divided by zero: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT INTERVAL '1' DAY / <divisor> AS result FROM VALUES (1), (0) AS t(n)
        """
      Then query error \[INTERVAL_DIVIDED_BY_ZERO\]

      Examples:
        | case                   | ansi  | divisor             |
        | int literal, ANSI on   | true  | 0                   |
        | int literal, ANSI off  | false | 0                   |
        | decimal literal        | false | 0.0                 |
        | double literal         | false | CAST(0 AS DOUBLE)   |
        | int column, ANSI on    | true  | n                   |
        | int column, ANSI off   | false | n                   |

    @sail-bug
    Scenario: try_divide of a day-time interval by zero returns NULL
      When query
        """
        SELECT try_divide(INTERVAL '1' DAY, 0) AS result
        """
      Then query result
        | result |
        | NULL   |

  # DivideDTInterval / MultiplyDTInterval round the microsecond result HALF_UP
  # (LongMath.divide, DoubleMath.roundToLong, BigDecimal.setScale(0, HALF_UP)).
  # Sail truncates toward zero.
  Rule: Day-time interval times or divided by a number rounds half up

    @sail-bug
    Scenario Outline: day-time interval rounding: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | expr                                           | result                                      |
        | divide by int                  | INTERVAL '0.000003' SECOND / 2                 | INTERVAL '0 00:00:00.000002' DAY TO SECOND  |
        | divide negative by int         | INTERVAL '-0.000001' SECOND / 2                | INTERVAL '-0 00:00:00.000001' DAY TO SECOND |
        | divide by double               | INTERVAL '0.000001' SECOND / CAST(2 AS DOUBLE) | INTERVAL '0 00:00:00.000001' DAY TO SECOND  |
        | divide by decimal              | INTERVAL '0.000001' SECOND / 2.0               | INTERVAL '0 00:00:00.000001' DAY TO SECOND  |
        | multiply by decimal            | INTERVAL '0.000001' SECOND * 2.5               | INTERVAL '0 00:00:00.000003' DAY TO SECOND  |
        | multiply by double             | INTERVAL '0.000001' SECOND * CAST(2.5 AS DOUBLE) | INTERVAL '0 00:00:00.000003' DAY TO SECOND |
        | multiply negative by double    | INTERVAL '0.000001' SECOND * CAST(-2.5 AS DOUBLE) | INTERVAL '-0 00:00:00.000003' DAY TO SECOND |

    @sail-bug
    Scenario: day-time interval rounding per row over columns
      When query
        """
        SELECT i / n AS q, i * f AS p
        FROM VALUES
          (INTERVAL '0.000001' SECOND, 2, 1.5),
          (INTERVAL '0.000003' SECOND, 2, 0.5),
          (INTERVAL '-0.000005' SECOND, 2, 2.5)
        AS t(i, n, f)
        """
      Then query result
        | q                                           | p                                           |
        | INTERVAL '0 00:00:00.000001' DAY TO SECOND  | INTERVAL '0 00:00:00.000002' DAY TO SECOND  |
        | INTERVAL '0 00:00:00.000002' DAY TO SECOND  | INTERVAL '0 00:00:00.000002' DAY TO SECOND  |
        | INTERVAL '-0 00:00:00.000003' DAY TO SECOND | INTERVAL '-0 00:00:00.000013' DAY TO SECOND |

  # MultiplyDTInterval uses Math.multiplyExact (integral) and roundToLong (fractional) with
  # no ANSI gate: an overflow raises "long overflow", a NaN/Infinity factor raises
  # "input is infinite or NaN". Sail silently wraps the integral product.
  Rule: Day-time interval multiplication overflow raises regardless of ANSI

    @sail-bug
    Scenario Outline: day-time interval multiplication overflow: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result FROM VALUES (INTERVAL '1' DAY), (INTERVAL '106751991' DAY) AS t(i)
        """
      Then query error <error>

      Examples:
        | case                       | ansi  | expr                                     | error                     |
        | literal, ANSI on           | true  | INTERVAL '1' DAY * 106751992             | long overflow             |
        | literal, ANSI off          | false | INTERVAL '1' DAY * 106751992             | long overflow             |
        | column, ANSI on            | true  | i * 2                                    | long overflow             |
        | column, ANSI off           | false | i * 2                                    | long overflow             |
        | NaN factor                 | false | INTERVAL '1' DAY * CAST('NaN' AS DOUBLE) | input is infinite or NaN  |
        | Infinity factor            | true  | i * CAST('Infinity' AS DOUBLE)           | input is infinite or NaN  |

    @sail-bug
    Scenario: try_multiply of a day-time interval overflow returns NULL
      When query
        """
        SELECT try_multiply(INTERVAL '106751991' DAY, 2) AS result
        """
      Then query result
        | result |
        | NULL   |

  # Adding, subtracting and negating day-time intervals use Math.*Exact and raise
  # INTERVAL_ARITHMETIC_OVERFLOW regardless of ANSI (Spark 4.2.0 arithmetic.scala).
  Rule: Day-time interval addition, subtraction and negation overflow

    @sail-bug
    Scenario Outline: day-time interval overflow: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.<suffix>\]

      Examples:
        | case                   | ansi  | expr                                                                                 | suffix             |
        | add, ANSI on           | true  | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND + INTERVAL '0.000001' SECOND     | WITH_SUGGESTION    |
        | add, ANSI off          | false | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND + INTERVAL '0.000001' SECOND     | WITH_SUGGESTION    |
        | subtract               | true  | INTERVAL '-106751991 04:00:54.775807' DAY TO SECOND - INTERVAL '0.000002' SECOND    | WITH_SUGGESTION    |
        | negate the minimum     | true  | -(make_dt_interval(-106751991, -4, 0, -54.775807) - INTERVAL '0.000001' SECOND)     | WITHOUT_SUGGESTION |

    @sail-bug
    Scenario Outline: negating the minimum day-time interval in a column overflows: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -i AS result
        FROM VALUES (make_dt_interval(1)), (make_dt_interval(-106751991, -4, 0, -54.775808)) AS t(i)
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITHOUT_SUGGESTION\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario: abs of the minimum day-time interval overflows
      When query
        """
        SELECT abs(i) AS result
        FROM VALUES (make_dt_interval(-1)), (make_dt_interval(-106751991, -4, 0, -54.775808)) AS t(i)
        """
      Then query error \[ARITHMETIC_OVERFLOW\]

    @sail-bug
    Scenario Outline: try_add and try_subtract of day-time intervals return NULL on overflow: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case         | expr                                                                                           |
        | try_add      | try_add(INTERVAL '106751991 04:00:54.775807' DAY TO SECOND, INTERVAL '0.000001' SECOND)        |
        | try_subtract | try_subtract(INTERVAL '-106751991 04:00:54.775807' DAY TO SECOND, INTERVAL '0.000002' SECOND)  |

  # Spark 4.2.0 Cast.scala: STRING -> day-time interval parses with
  # IntervalUtils.castStringToDTInterval (sub-microsecond digits are dropped); a malformed
  # string raises INVALID_INTERVAL_FORMAT in both ANSI modes, and try_cast gives NULL.
  # Sail fails in its SQL parser for any string operand.
  Rule: A string casts to a day-time interval

    @sail-bug
    Scenario Outline: string to day-time interval cast: <case>
      When query
        """
        SELECT CAST(<expr> AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('1 02:03:04'), ('-2 10:00:00.5') AS t(s)
        """
      Then query result
        | result   |
        | <r1>     |
        | <r2>     |

      Examples:
        | case                          | expr                    | r1                                         | r2                                         |
        | column                        | s                       | INTERVAL '1 02:03:04' DAY TO SECOND        | INTERVAL '-2 10:00:00.5' DAY TO SECOND     |
        | literal drops sub-micro digits | '1 02:03:04.123456789' | INTERVAL '1 02:03:04.123456' DAY TO SECOND | INTERVAL '1 02:03:04.123456' DAY TO SECOND |

    @sail-bug
    Scenario Outline: a malformed string to day-time interval cast: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(s AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('abc'), ('1 02:03:04') AS t(s)
        """
      Then query error \[INVALID_INTERVAL_FORMAT\.UNMATCHED_FORMAT_STRING_WITH_NOTICE\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario: try_cast of a malformed string to a day-time interval returns NULL
      When query
        """
        SELECT try_cast(s AS INTERVAL DAY TO SECOND) AS result FROM VALUES ('abc'), ('1 02:03:04') AS t(s)
        """
      Then query result
        | result                              |
        | NULL                                |
        | INTERVAL '1 02:03:04' DAY TO SECOND |

  # Spark 4.2.0 Cast.scala: a day-time interval casts to an integral in units of its END
  # field (IntervalUtils.dayTimeIntervalToLong); a number casts to a day-time interval in
  # units of the target's end field and raises CAST_OVERFLOW (both ANSI modes) when it
  # does not fit. Sail always counts seconds and wraps.
  Rule: Casts between day-time intervals and numbers

    Scenario: a DAY TO SECOND interval casts to BIGINT seconds
      When query
        """
        SELECT CAST(INTERVAL '1 02:03:04' DAY TO SECOND AS BIGINT) AS result
        """
      Then query result
        | result |
        | 93784  |

    @sail-bug
    Scenario Outline: a single-unit day-time interval casts to INT in its own unit: <case>
      When query
        """
        SELECT CAST(<expr> AS INT) AS result FROM VALUES (INTERVAL '2' DAY), (INTERVAL '-5' DAY) AS t(i)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case            | expr                | r1 | r2 |
        | day column      | i                   | 2  | -5 |
        | hour literal    | INTERVAL '2' HOUR   | 2  | 2  |
        | minute literal  | INTERVAL '90' MINUTE | 90 | 90 |

    Scenario: a number casts to DAY TO SECOND as seconds
      When query
        """
        SELECT CAST(CAST(n AS INTERVAL DAY TO SECOND) AS STRING) AS result FROM VALUES (2), (-3) AS t(n)
        """
      Then query result
        | result                               |
        | INTERVAL '0 00:00:02' DAY TO SECOND  |
        | INTERVAL '-0 00:00:03' DAY TO SECOND |

    @sail-bug
    Scenario Outline: a BIGINT that does not fit INTERVAL DAY: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(n AS INTERVAL DAY) AS result
        FROM VALUES (CAST(1 AS BIGINT)), (CAST(9223372036854775807 AS BIGINT)) AS t(n)
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

    @sail-bug
    Scenario: try_cast of a BIGINT that does not fit INTERVAL DAY returns NULL
      When query
        """
        SELECT CAST(try_cast(CAST(9223372036854775807 AS BIGINT) AS INTERVAL DAY) AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |
