Feature: TIME subtraction result parity

  # `SubtractTimes` returns `DayTimeIntervalType(HOUR, SECOND)` (`timeExpressions.scala:626`).
  # Sail casts the difference to Arrow `Duration`, which keeps the value and the day-time
  # family but cannot carry the HOUR TO SECOND start/end fields.
  @sail-bug
  @spark-4.1
  Scenario: TIME subtraction preserves Spark's HOUR TO SECOND interval subtype
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        typeof(
          CAST(TIME '12:00:00.000001' AS TIME(6)) -
          CAST(TIME '12:00:01.250' AS TIME(3))
        ) AS result_type,
        CAST(
          CAST(TIME '12:00:00.000001' AS TIME(6)) -
          CAST(TIME '12:00:01.250' AS TIME(3))
          AS STRING
        ) AS result
      """
    Then query result
      | result_type             | result                                     |
      | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |

  # A TIME difference is a day-time interval, so Spark feeds it straight back into
  # `TimeAddInterval` (`BinaryArithmeticWithDatetimeResolver.scala:87,133`). Sail spells that
  # interval as Arrow `Duration`, which DataFusion's `time +- interval` rule does not match.
  @spark-4.1
  Scenario: a TIME difference composes back with a TIME
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        CAST(TIME '12:00:00' + (TIME '12:00:00' - TIME '01:00:00') AS STRING) AS added,
        CAST(TIME '12:00:00' - (TIME '12:00:00' - TIME '01:00:00') AS STRING) AS subtracted
      """
    Then query result
      | added    | subtracted |
      | 23:00:00 | 01:00:00   |

  @spark-4.1
  Scenario: a TIME takes a day-time interval in either order
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        CAST(TIME '12:00:00' + INTERVAL '1' HOUR AS STRING) AS a,
        CAST(INTERVAL '1' HOUR + TIME '12:00:00' AS STRING) AS b,
        CAST(TIME '12:00:00' - INTERVAL '1' HOUR AS STRING) AS c
      """
    Then query result
      | a        | b        | c        |
      | 13:00:00 | 13:00:00 | 11:00:00 |

  # NOT this PR's work -- the fix belongs with the ANSI/overflow PR. Pinned here because the
  # `TIME +- interval` arms above turn a hard error into a WRONG VALUE: DataFusion wraps within
  # the 24-hour clock, Spark raises `[DATETIME_OVERFLOW]` in both ANSI modes
  # (`DateTimeUtils.scala:1098-1104`, from `timeExpressions.scala:594`). Every direction the wrap
  # can go is pinned, not just one, so the overflow PR has a complete red target.
  #
  # A guard cannot land here: Sail's `typeof` EVALUATES its argument (see the scenario below), so
  # raising on overflow would also turn red the `typeof(...) IS NOT NULL` rows this same matrix
  # uses to assert resolution. The guard and those rows have to move together.
  @sail-bug
  @spark-4.1
  Scenario Outline: TIME arithmetic that leaves the day overflows: <case>
    Given config spark.sql.timeType.enabled = true
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <expression> AS result
      """
    Then query error (?i)DATETIME_OVERFLOW

    Examples:
      | case                     | ansi  | expression                                            |
      | past midnight ansi-off   | false | TIME '23:30:00' + INTERVAL '2' HOUR                   |
      | past midnight ansi-on    | true  | TIME '23:30:00' + INTERVAL '2' HOUR                   |
      | before midnight          | true  | TIME '01:00:00' - INTERVAL '2' HOUR                   |
      | a whole day              | true  | TIME '12:00:00' + INTERVAL '2' DAY                    |
      | a TIME difference        | true  | TIME '23:00:00' + (TIME '12:00:00' - TIME '01:00:00') |
      | the interval first       | true  | INTERVAL '2' HOUR + TIME '23:30:00'                   |

  # Spark's `TypeOf` reports the type without evaluating the expression; Sail's evaluates it, so a
  # runtime failure escapes from a query that only asked what the type would be. This is why the
  # overflow guard above cannot be added without moving the resolution matrix's probe too.
  @sail-bug
  Scenario: typeof reports the type without evaluating the expression
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT typeof(CAST(1 AS INT) / CAST(0 AS INT)) AS result
      """
    Then query result
      | result |
      | double |

  # `extract` reads a day-time interval field by field: `getMinutes` is the minutes past the hour
  # (`IntervalUtils.scala:60-62`). Sail's `date_part` reads a `Duration` as a total, and a TIME
  # difference is a `Duration` now, so it answers 90.
  @sail-bug
  @spark-4.1
  Scenario: the minutes of a TIME difference are the minutes past the hour
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT extract(MINUTE FROM TIME '10:30:00' - TIME '09:00:00') AS minutes
      """
    Then query result
      | minutes |
      | 30      |

  # A TIME whose bounds the planner knows -- one projected out of a CTE -- sent DataFusion's interval
  # bound propagation into `unreachable!()` for `time + interval`, and the query died with a
  # cancelled stream.
  @spark-4.1
  Scenario Outline: a TIME from a CTE takes a day-time interval: <case> with ANSI <ansi>
    Given config spark.sql.timeType.enabled = true
    And config spark.sql.ansi.enabled = <ansi>
    When query
      """
      WITH w AS (SELECT TIME '01:00:00' AS c) SELECT CAST(<expression> AS STRING) AS result FROM w
      """
    Then query result
      | result     |
      | <expected> |

    Examples:
      | case               | ansi  | expression              | expected |
      | plus               | false | c + INTERVAL '1' HOUR   | 02:00:00 |
      | plus               | true  | c + INTERVAL '1' HOUR   | 02:00:00 |
      | minus              | true  | c - INTERVAL '1' MINUTE | 00:59:00 |
      | the interval first | true  | INTERVAL '1' HOUR + c   | 02:00:00 |

