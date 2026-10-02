Feature: TIME functions (make_time, time_diff, time_trunc)

  # `spark.sql.timeType.enabled` is an internal Spark configuration whose default is
  # `Utils.isTesting`, so it is on only inside Spark's own test suite and off in every real
  # session. Without it, `TimeExpression.checkInputDataTypes()` rejects these functions with
  # `UNSUPPORTED_TIME_TYPE` and the scenarios below cannot be validated against JVM Spark.
  Background:
      Given config spark.sql.timeType.enabled = true

  Rule: make_time

    Scenario Outline: make_time: <case>
      When query
        """
        SELECT make_time(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                       | args              | result          |
        | basic make_time            | 6, 30, 45.887     | 06:30:45.887    |
        | make_time midnight         | 0, 0, 0           | 00:00:00        |
        | make_time max precision    | 23, 59, 59.999999 | 23:59:59.999999 |
        | make_time integer seconds  | 12, 0, 30         | 12:00:30        |
        | make_time NULL propagation | NULL, 30, 0       | NULL            |

    Scenario Outline: make_time invalid: <case>
      When query
        """
        SELECT CAST(make_time(<args>) AS STRING)
        """
      Then query error <error>

      Examples:
        | case                            | args     | error          |
        | make_time invalid hour errors   | 25, 0, 0 | HourOfDay      |
        | make_time invalid minute errors | 0, 60, 0 | MinuteOfHour   |
        | make_time invalid second errors | 0, 0, 60 | SecondOfMinute |

  Rule: time_diff

    Scenario Outline: time_diff: <case>
      When query
        """
        SELECT time_diff(<unit>, <start>, <end>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | unit          | start           | end                 | result  |
        | time_diff hours exact                   | 'HOUR'        | TIME '20:30:29' | TIME '21:30:29'     | 1       |
        | time_diff hours truncation              | 'HOUR'        | TIME '20:30:29' | TIME '21:30:28'     | 0       |
        | time_diff negative                      | 'HOUR'        | TIME '20:30:29' | TIME '12:00:00'     | -8      |
        | time_diff minutes                       | 'MINUTE'      | TIME '10:00:00' | TIME '10:45:30'     | 45      |
        | time_diff seconds                       | 'SECOND'      | TIME '10:00:00' | TIME '10:00:30'     | 30      |
        | time_diff microseconds                  | 'MICROSECOND' | TIME '00:00:00' | TIME '00:00:01'     | 1000000 |
        | time_diff milliseconds                  | 'MILLISECOND' | TIME '00:00:00' | TIME '00:00:01.500' | 1500    |
        | time_diff NULL start propagates to NULL | 'HOUR'        | NULL            | TIME '01:00:00'     | NULL    |
        | time_diff NULL end propagates to NULL   | 'MINUTE'      | TIME '10:00:00' | NULL                | NULL    |
        | time_diff NULL unit propagates to NULL  | NULL          | TIME '10:00:00' | TIME '11:00:00'     | NULL    |

    Scenario: time_diff invalid unit errors
      When query
        """
        SELECT time_diff('MS', TIME '10:00:00', TIME '11:00:00')
        """
      Then query error expects one of the units

    Scenario: time_diff with unit from column
      When query
        """
        SELECT time_diff(unit, TIME '08:00:00', TIME '10:30:00') AS result
        FROM (VALUES ('HOUR'), ('MINUTE')) AS t(unit)
        """
      Then query result
        | result |
        | 2      |
        | 150    |

    Scenario: time_diff unit from column is case-insensitive across rows
      When query
      """
      SELECT time_diff(unit, TIME '08:00:00', TIME '10:30:00') AS result
      FROM (VALUES ('hour'), ('Hour'), ('HOUR'), ('minute'), ('MiNuTe')) AS t(unit)
      """
      Then query result
      | result |
      | 2      |
      | 2      |
      | 2      |
      | 150    |
      | 150    |

    Scenario: time_diff mixes unit and times per row
      When query
      """
      SELECT time_diff(unit, s, e) AS result
      FROM (VALUES
        ('hour',        TIME '00:00:00', TIME '05:30:00'),
        ('MINUTE',      TIME '10:00:00', TIME '10:45:00'),
        ('sEcOnD',      TIME '00:00:00', TIME '00:00:30'),
        ('Millisecond', TIME '00:00:00', TIME '00:00:01.500'),
        ('microsecond', TIME '00:00:00', TIME '00:00:00.000123')
      ) AS t(unit, s, e)
      """
      Then query result
      | result |
      | 5      |
      | 45     |
      | 30     |
      | 1500   |
      | 123    |

    Scenario: time_diff unit from column with interleaved NULL
      When query
      """
      SELECT time_diff(unit, TIME '01:00:00', TIME '03:00:00') AS result
      FROM (VALUES ('hour'), (CAST(NULL AS STRING)), ('Hour')) AS t(unit)
      """
      Then query result
      | result |
      | 2      |
      | NULL   |
      | 2      |

  Rule: time_trunc

    Scenario Outline: time_trunc: <case>
      When query
        """
        SELECT time_trunc(<unit>, <time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | unit          | time                   | result          |
        | time_trunc hour                         | 'HOUR'        | TIME '09:32:05.359'    | 09:00:00        |
        | time_trunc minute                       | 'MINUTE'      | TIME '09:32:05.359'    | 09:32:00        |
        | time_trunc second                       | 'SECOND'      | TIME '09:32:05.359'    | 09:32:05        |
        | time_trunc millisecond                  | 'MILLISECOND' | TIME '09:32:05.123456' | 09:32:05.123    |
        | time_trunc microsecond passthrough      | 'MICROSECOND' | TIME '09:32:05.123456' | 09:32:05.123456 |
        | time_trunc NULL unit propagates to NULL | NULL          | TIME '09:32:05.123456' | NULL            |
        | time_trunc NULL time propagates to NULL | 'HOUR'        | NULL                   | NULL            |

    Scenario: time_trunc invalid unit errors
      When query
        """
        SELECT CAST(time_trunc('MS', TIME '09:32:05.123456') AS STRING)
        """
      Then query error expects one of the units

    Scenario: time_trunc with unit from column
      When query
        """
        SELECT time_trunc(unit, TIME '09:32:05.359') AS result
        FROM (VALUES ('HOUR'), ('MINUTE'), ('SECOND')) AS t(unit)
        """
      Then query result
        | result   |
        | 09:00:00 |
        | 09:32:00 |
        | 09:32:05 |

  # Every expected value below was measured on Spark 4.2.0 (JVM, UTC); the rules are those of
  # Spark 4.2.0 `timeExpressions.scala` and `DateTimeUtils.scala`.
  @spark-4.1
  Rule: make_time over columns and out-of-range fields

    Scenario: make_time over columns with distinct rows
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT make_time(h, m, s) AS r FROM VALUES
          (0, 0, 0.0), (9, 5, 3.5), (23, 59, 59.999999), (NULL, 1, 1.0) AS x(h, m, s)
        """
      Then query schema
        """
        root
         |-- r: time(6) (nullable = true)
        """
      And query result
        | r               |
        | 00:00:00        |
        | 09:05:03.5      |
        | 23:59:59.999999 |
        | NULL            |

    # `DateTimeUtils.makeTime` raises DATETIME_FIELD_OUT_OF_BOUNDS whatever the ANSI mode. A
    # seconds value that rounds up to 60 in DECIMAL(16, 6) is out of range too.
    @sail-bug
    Scenario Outline: make_time rejects an out-of-range field: <case> (ANSI <ansi>)
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_time(<args>) AS r
        """
      Then query error DATETIME_FIELD_OUT_OF_BOUNDS

      Examples:
        | case                        | ansi  | args              |
        | a negative hour             | true  | -1, 0, 0          |
        | an hour beyond 23           | false | 25, 0, 0          |
        | negative seconds            | true  | 12, 0, -1         |
        | seconds that round up to 60 | true  | 12, 0, 59.9999999 |

  @spark-4.1
  Rule: time_trunc keeps the TIME precision

    Scenario: time_trunc over a TIME column with distinct rows
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_trunc('HOUR', t) AS r FROM VALUES
          (TIME '00:59:59.999999'), (TIME '13:05:03.5'), (NULL) AS x(t)
        """
      Then query result
        | r        |
        | 00:00:00 |
        | 13:00:00 |
        | NULL     |

    # `TimeTrunc.dataType` is `time.dataType`.
    @sail-bug
    Scenario: time_trunc returns the precision of its TIME argument
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT
          time_trunc('HOUR', CAST(TIME '13:05:03.5' AS TIME(3))) AS p3,
          time_trunc('SECOND', CAST(TIME '13:05:03.5' AS TIME(0))) AS p0
        """
      Then query schema
        """
        root
         |-- p3: time(3) (nullable = true)
         |-- p0: time(0) (nullable = true)
        """
      And query result
        | p3       | p0       |
        | 13:00:00 | 13:05:03 |

  @spark-4.1
  Rule: Field extraction from TIME

    Scenario: hour, minute and second of a TIME column
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT hour(t) AS h, minute(t) AS m, second(t) AS s, extract(SECOND FROM t) AS fs FROM VALUES
          (TIME '00:00:00'), (TIME '09:05:03.5'), (TIME '23:59:59.999999'), (NULL) AS x(t)
        """
      Then query result
        | h    | m    | s    | fs        |
        | 0    | 0    | 0    | 0.000000  |
        | 9    | 5    | 3    | 3.500000  |
        | 23   | 59   | 59   | 59.999999 |
        | NULL | NULL | NULL | NULL      |

    # `HoursOfTime` / `MinutesOfTime` are `StaticInvoke` replacements, which are nullable.
    @sail-bug
    Scenario: extract of HOUR and MINUTE from a TIME literal is nullable
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT extract(HOUR FROM TIME '09:05:03.5') AS h, date_part('MINUTE', TIME '09:05:03.5') AS m
        """
      Then query schema
        """
        root
         |-- h: integer (nullable = true)
         |-- m: integer (nullable = true)
        """

    @sail-bug
    Scenario: extract of a date field from TIME is rejected at analysis
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT extract(DAY FROM TIME '09:05:03.5') AS r
        """
      Then query error INVALID_EXTRACT_FIELD

  @spark-4.1
  Rule: current_time precision

    @sail-bug
    Scenario: current_time defaults to microseconds and honours an explicit precision
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT typeof(current_time()) AS default_precision, typeof(current_time(3)) AS p3, typeof(current_time(0)) AS p0
        """
      Then query result
        | default_precision | p3      | p0      |
        | time(6)           | time(3) | time(0) |

    @sail-bug
    Scenario: current_time rejects a precision beyond 6
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT current_time(7) AS r
        """
      Then query error VALUE_OUT_OF_RANGE

  @spark-4.1
  Rule: time_from_* and time_to_* conversions

    @sail-bug
    Scenario: time_from_seconds, time_from_millis and time_from_micros build a TIME
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_from_seconds(45296.5) AS s, time_from_millis(45296500) AS ms, time_from_micros(45296500000) AS us
        """
      Then query schema
        """
        root
         |-- s: time(6) (nullable = true)
         |-- ms: time(6) (nullable = true)
         |-- us: time(6) (nullable = true)
        """
      And query result
        | s          | ms         | us         |
        | 12:34:56.5 | 12:34:56.5 | 12:34:56.5 |

    @sail-bug
    Scenario: time_from_seconds over a column with distinct rows
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_from_seconds(s) AS r FROM VALUES (0), (45296), (86399) AS x(s)
        """
      Then query result
        | r        |
        | 00:00:00 |
        | 12:34:56 |
        | 23:59:59 |

    @sail-bug
    Scenario Outline: time_from_seconds rejects a value outside the day: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_from_seconds(<value>) AS r
        """
      Then query error DATETIME_FIELD_OUT_OF_BOUNDS

      Examples:
        | case              | value |
        | a full day        | 86400 |
        | a negative second | -1    |

    @sail-bug
    Scenario: time_to_seconds, time_to_millis and time_to_micros count from midnight
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_to_seconds(TIME '12:34:56.5') AS s, time_to_millis(TIME '12:34:56.5') AS ms, time_to_micros(TIME '12:34:56.5') AS us
        """
      Then query schema
        """
        root
         |-- s: decimal(14,6) (nullable = true)
         |-- ms: long (nullable = true)
         |-- us: long (nullable = true)
        """
      And query result
        | s            | ms       | us          |
        | 45296.500000 | 45296500 | 45296500000 |

    @sail-bug
    Scenario: time_to_micros over a column with distinct rows
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT time_to_micros(t) AS r FROM VALUES (TIME '00:00:00'), (TIME '09:05:03.5'), (NULL) AS x(t)
        """
      Then query result
        | r           |
        | 0           |
        | 32703500000 |
        | NULL        |
