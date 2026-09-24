Feature: TIME subtraction result parity

  # `SubtractTimes` returns `DayTimeIntervalType(HOUR, SECOND)` (`timeExpressions.scala:626`).
  # Sail keeps the microsecond Duration and carries HOUR TO SECOND in field metadata,
  # including through projections and unary negation. Scaling returns DAY TO SECOND.
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

  @function(nullability)
  @spark-4.1
  Scenario: TIME subtraction keeps Spark's schema
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.timeType.enabled = true
    When query
      """
      SELECT TIME'12:00:00.000001' - TIME'12:00:01.250' AS result
      """
    Then query schema
      """
      root
       |-- result: interval hour to second (nullable = false)
      """

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

  # `TimeAddInterval` returns `TimeType(max(p, 6))` when the interval reaches SECOND
  # (`timeExpressions.scala:596-606`), so the fraction of the interval is not cut back to the
  # digits of the TIME.
  @spark-4.1
  Scenario: a TIME(0) or TIME(3) shifted by a fractional interval keeps the fraction
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        CAST(CAST(TIME '12:00:00' AS TIME(0)) + INTERVAL '0.5' SECOND AS STRING) AS added,
        CAST(INTERVAL '0.5' SECOND + CAST(TIME '12:00:00' AS TIME(0)) AS STRING) AS reversed,
        CAST(CAST(TIME '12:00:00' AS TIME(0)) - INTERVAL '0.5' SECOND AS STRING) AS subtracted,
        CAST(CAST(TIME '12:00:00' AS TIME(3)) + INTERVAL '0.0005' SECOND AS STRING) AS millis,
        typeof(CAST(TIME '12:00:00' AS TIME(0)) + INTERVAL '0.5' SECOND) AS result_type
      """
    Then query result
      | added      | reversed   | subtracted | millis        | result_type |
      | 12:00:00.5 | 12:00:00.5 | 11:59:59.5 | 12:00:00.0005 | time(6)     |

  @spark-4.1
  Scenario: a TIME(0) column shifted by a fractional interval keeps the fraction on every row
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT CAST(t + INTERVAL '0.25' SECOND AS STRING) AS r
      FROM (
        SELECT id, CAST(make_time(12, 0, CAST(id AS DECIMAL(16, 6))) AS TIME(0)) AS t
        FROM range(0, 4, 1, 2)
      )
      ORDER BY id
      """
    Then query result ordered
      | r           |
      | 12:00:00.25 |
      | 12:00:01.25 |
      | 12:00:02.25 |
      | 12:00:03.25 |

  # Spark rejects overflow in both ANSI modes (DateTimeUtils.scala:1098-1104).
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

  # Spark's `TypeOf` reports the type without evaluating the expression. Sail's evaluates it, which
  # is why a division by zero used to escape from a query that only asked what the type would be;
  # the divisor is no longer refused at analysis, so the plan answers the type like Spark's.
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
  # (`IntervalUtils.scala:60-62`). A TIME difference is a day-time interval, so it answers 30, not the
  # 90 minutes of the total.
  @spark-4.1
  Scenario: the minutes of a TIME difference are the minutes past the hour
    Given config spark.sql.timeType.enabled = true
    When query
      """
      SELECT
        extract(MINUTE FROM TIME '10:30:00' - TIME '09:00:00') AS minutes,
        date_part('MINUTE', TIME '10:30:00' - TIME '09:00:00') AS part,
        extract(MINUTE FROM (TIME '10:30:00' - TIME '09:00:00') * 2) AS doubled,
        extract(MINUTE FROM -(TIME '10:30:00' - TIME '09:00:00')) AS negated,
        extract(HOUR FROM TIME '10:30:00' - TIME '09:00:00') AS hours
      """
    Then query result
      | minutes | part | doubled | negated | hours |
      | 30      | 30   | 0       | -30     | 1     |

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


  @spark-4.1
  Scenario Outline: TIME difference keeps its declared range with ANSI <ansi>: <query>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.timeType.enabled = true
    When query
      """
      <query>
      """
    Then query result
      | t | v |
      | <type> | <value> |

    Examples:
      | ansi | query | type | value |
      | false | SELECT typeof(TIME'12:00:00.000001' - TIME'12:00:01.250') AS t, CAST(TIME'12:00:00.000001' - TIME'12:00:01.250' AS STRING) AS v | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'12:00:00.000001' - TIME'12:00:01.250' AS d) | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |
      | false | SELECT typeof(TIME'01:00:00' - NULL) AS t, CAST(TIME'01:00:00' - NULL AS STRING) AS v | interval hour to second | NULL |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'01:00:00' - NULL AS d) | interval hour to second | NULL |
      | false | SELECT typeof(NULL - TIME'01:00:00') AS t, CAST(NULL - TIME'01:00:00' AS STRING) AS v | interval hour to second | NULL |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT NULL - TIME'01:00:00' AS d) | interval hour to second | NULL |
      | false | SELECT typeof(TIME'01:00:00' - TIME'01:00:00') AS t, CAST(TIME'01:00:00' - TIME'01:00:00' AS STRING) AS v | interval hour to second | INTERVAL '00:00:00' HOUR TO SECOND |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'01:00:00' - TIME'01:00:00' AS d) | interval hour to second | INTERVAL '00:00:00' HOUR TO SECOND |
      | false | SELECT typeof((TIME'06:00:00' - TIME'01:00:00') * 2) AS t, CAST((TIME'06:00:00' - TIME'01:00:00') * 2 AS STRING) AS v | interval day to second | INTERVAL '0 10:00:00' DAY TO SECOND |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT (TIME'06:00:00' - TIME'01:00:00') * 2 AS d) | interval day to second | INTERVAL '0 10:00:00' DAY TO SECOND |
      | false | SELECT typeof(-(TIME'06:00:00' - TIME'01:00:00')) AS t, CAST(-(TIME'06:00:00' - TIME'01:00:00') AS STRING) AS v | interval hour to second | INTERVAL '-05:00:00' HOUR TO SECOND |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT -(TIME'06:00:00' - TIME'01:00:00') AS d) | interval hour to second | INTERVAL '-05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(TIME'12:00:00.000001' - TIME'12:00:01.250') AS t, CAST(TIME'12:00:00.000001' - TIME'12:00:01.250' AS STRING) AS v | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'12:00:00.000001' - TIME'12:00:01.250' AS d) | interval hour to second | INTERVAL '-00:00:01.249999' HOUR TO SECOND |
      | true | SELECT typeof('06:00:00' - TIME'01:00:00') AS t, CAST('06:00:00' - TIME'01:00:00' AS STRING) AS v | interval hour to second | INTERVAL '05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT '06:00:00' - TIME'01:00:00' AS d) | interval hour to second | INTERVAL '05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(TIME'06:00:00' - '01:00:00') AS t, CAST(TIME'06:00:00' - '01:00:00' AS STRING) AS v | interval hour to second | INTERVAL '05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'06:00:00' - '01:00:00' AS d) | interval hour to second | INTERVAL '05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(TIME'01:00:00' - NULL) AS t, CAST(TIME'01:00:00' - NULL AS STRING) AS v | interval hour to second | NULL |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'01:00:00' - NULL AS d) | interval hour to second | NULL |
      | true | SELECT typeof(NULL - TIME'01:00:00') AS t, CAST(NULL - TIME'01:00:00' AS STRING) AS v | interval hour to second | NULL |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT NULL - TIME'01:00:00' AS d) | interval hour to second | NULL |
      | true | SELECT typeof(TIME'01:00:00' - TIME'01:00:00') AS t, CAST(TIME'01:00:00' - TIME'01:00:00' AS STRING) AS v | interval hour to second | INTERVAL '00:00:00' HOUR TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'01:00:00' - TIME'01:00:00' AS d) | interval hour to second | INTERVAL '00:00:00' HOUR TO SECOND |
      | true | SELECT typeof((TIME'06:00:00' - TIME'01:00:00') * 2) AS t, CAST((TIME'06:00:00' - TIME'01:00:00') * 2 AS STRING) AS v | interval day to second | INTERVAL '0 10:00:00' DAY TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT (TIME'06:00:00' - TIME'01:00:00') * 2 AS d) | interval day to second | INTERVAL '0 10:00:00' DAY TO SECOND |
      | true | SELECT typeof(-(TIME'06:00:00' - TIME'01:00:00')) AS t, CAST(-(TIME'06:00:00' - TIME'01:00:00') AS STRING) AS v | interval hour to second | INTERVAL '-05:00:00' HOUR TO SECOND |
      | true | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT -(TIME'06:00:00' - TIME'01:00:00') AS d) | interval hour to second | INTERVAL '-05:00:00' HOUR TO SECOND |

  @spark-4.1
  Scenario Outline: TIME difference rejects with ANSI <ansi>: <query>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.timeType.enabled = true
    When query
      """
      <query>
      """
    Then query error (?i)cannot resolve

    Examples:
      | ansi | query |
      | false | SELECT typeof('06:00:00' - TIME'01:00:00') AS t, CAST('06:00:00' - TIME'01:00:00' AS STRING) AS v |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT '06:00:00' - TIME'01:00:00' AS d) |
      | false | SELECT typeof(TIME'06:00:00' - '01:00:00') AS t, CAST(TIME'06:00:00' - '01:00:00' AS STRING) AS v |
      | false | SELECT typeof(d) AS t, CAST(d AS STRING) AS v FROM (SELECT TIME'06:00:00' - '01:00:00' AS d) |
