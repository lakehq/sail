Feature: TIME data type support

  Rule: TIME literal syntax

    Scenario Outline: Literal: <case>
      When query
        """
        SELECT TIME <lit> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | lit               | result          |
        | basic time literal              | '10:30:45'        | 10:30:45        |
        | time with microseconds          | '14:25:36.123456' | 14:25:36.123456 |
        | midnight                        | '00:00:00'        | 00:00:00        |
        | one microsecond before midnight | '23:59:59.999999' | 23:59:59.999999 |

  Rule: TIME in table operations

    Scenario: select from table with TIME column
      When query
        """
        SELECT * FROM VALUES
          (TIME '09:00:00'),
          (TIME '12:30:00'),
          (TIME '18:45:00')
        AS t(time_col)
        ORDER BY time_col
        """
      Then query result ordered
        | time_col |
        | 09:00:00 |
        | 12:30:00 |
        | 18:45:00 |

    Scenario: filter by TIME value
      When query
        """
        SELECT time_col FROM VALUES
          (TIME '08:00:00'),
          (TIME '12:00:00'),
          (TIME '16:00:00')
        AS t(time_col)
        WHERE time_col > TIME '10:00:00'
        ORDER BY time_col
        """
      Then query result ordered
        | time_col |
        | 12:00:00 |
        | 16:00:00 |

  Rule: NULL handling

    Scenario: TIME column with NULLs
      When query
        """
        SELECT time_col FROM VALUES
          (TIME '10:00:00'),
          (NULL),
          (TIME '14:00:00')
        AS t(time_col)
        WHERE time_col IS NOT NULL
        ORDER BY time_col
        """
      Then query result ordered
        | time_col |
        | 10:00:00 |
        | 14:00:00 |

  Rule: Precision levels

    Scenario Outline: Precision: <case>
      When query
        """
        SELECT TIME <lit> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | lit               | result          |
        | second precision      | '12:34:56'        | 12:34:56        |
        | millisecond precision | '12:34:56.123'    | 12:34:56.123    |
        | microsecond precision | '12:34:56.123456' | 12:34:56.123456 |

  # Every expected value below was measured on Spark 4.2.0 (JVM, UTC). The TIME rules come from
  # Spark 4.2.0 `BinaryArithmeticWithDatetimeResolver.scala` (the `TimeType` arms of Add and
  # Subtract), `timeExpressions.scala` (`TimeAddInterval`, `SubtractTimes`) and `Cast.scala`
  # (`canCast` / `canAnsiCast`: TIME casts only to STRING, TIME(p), integral and decimal types,
  # and only STRING casts to TIME).

  @spark-4.1
  Rule: TIME literal limits

    Scenario: TIME literal truncates fractional digits beyond microseconds
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME '12:34:56.1234567' AS a, TIME '23:59:59.9999999' AS b
        """
      Then query schema
        """
        root
         |-- a: time(6) (nullable = false)
         |-- b: time(6) (nullable = false)
        """
      And query result
        | a               | b               |
        | 12:34:56.123456 | 23:59:59.999999 |

    # Spark 4.2.0 `AstBuilder.visitTypeConstructor` raises INVALID_TYPED_LITERAL at parse time.
    @sail-bug
    Scenario Outline: TIME literal rejects <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT TIME <lit> AS r
        """
      Then query error INVALID_TYPED_LITERAL

      Examples:
        | case                     | lit        |
        | the hour 24              | '24:00:00' |
        | an hour beyond 23        | '25:00:00' |
        | a minute beyond 59       | '12:60:00' |
        | a string that is no time | 'garbage'  |

  @spark-4.1
  Rule: TIME plus or minus a day-time interval

    # `TimeAddInterval` (Spark 4.2.0 timeExpressions.scala): the result stays inside [00:00, 24:00).
    @sail-bug
    Scenario Outline: TIME and a day-time interval: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r
        """
      Then query schema
        """
        root
         |-- r: time(6) (nullable = true)
        """
      And query result
        | r        |
        | <result> |

      Examples:
        | case                               | expr                                                  | result          |
        | time plus an hour interval         | TIME '12:00:00' + INTERVAL '3' HOUR                   | 15:00:00        |
        | time minus an hour interval        | TIME '12:00:00' - INTERVAL '3' HOUR                   | 09:00:00        |
        | an hour interval plus time         | INTERVAL '3' HOUR + TIME '12:00:00'                   | 15:00:00        |
        | time plus a fractional second      | TIME '12:00:00' + INTERVAL '1.5' SECOND               | 12:00:01.5      |
        | time plus a day to second interval | TIME '12:00:00' + INTERVAL '0 01:00:00' DAY TO SECOND | 13:00:00        |
        | time reaching the last microsecond | TIME '23:59:59.999998' + INTERVAL '0.000001' SECOND   | 23:59:59.999999 |

    @sail-bug
    Scenario: TIME column plus a day-time interval column
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT t + i AS r FROM VALUES
          (TIME '00:00:00', INTERVAL '01:02:03' HOUR TO SECOND),
          (TIME '09:05:03.5', INTERVAL '30' MINUTE),
          (TIME '23:00:00', INTERVAL '-23' HOUR),
          (NULL, INTERVAL '1' HOUR),
          (TIME '10:00:00', NULL)
          AS x(t, i)
        """
      Then query schema
        """
        root
         |-- r: time(6) (nullable = true)
        """
      And query result
        | r          |
        | 01:02:03   |
        | 09:35:03.5 |
        | 00:00:00   |
        | NULL       |
        | NULL       |

    # The result precision is max(time precision, 0 if the interval ends before SECOND else 6).
    @sail-bug
    Scenario: TIME plus a day-time interval keeps or widens the TIME precision
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          typeof(CAST(TIME '10:00:00' AS TIME(0)) + INTERVAL '1' HOUR) AS hour_on_p0,
          typeof(CAST(TIME '10:00:00' AS TIME(0)) + INTERVAL '1' SECOND) AS second_on_p0,
          typeof(CAST(TIME '10:00:00' AS TIME(3)) + INTERVAL '1' HOUR) AS hour_on_p3,
          CAST(TIME '10:00:00' AS TIME(0)) + INTERVAL '1.5' SECOND AS value_on_p0
        """
      Then query result
        | hour_on_p0 | second_on_p0 | hour_on_p3 | value_on_p0 |
        | time(0)    | time(6)      | time(3)    | 10:00:01.5  |

    # `DateTimeUtils.timeAddInterval` throws DATETIME_OVERFLOW whatever the ANSI mode.
    @sail-bug
    Scenario Outline: TIME and a day-time interval overflow past midnight: <case> (ANSI <ansi>)
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS r
        """
      Then query error DATETIME_OVERFLOW

      Examples:
        | case                              | ansi  | expr                                                |
        | forward past midnight             | true  | TIME '23:00:00' + INTERVAL '2' HOUR                 |
        | forward past midnight             | false | TIME '23:00:00' + INTERVAL '2' HOUR                 |
        | backward before midnight          | true  | TIME '01:00:00' - INTERVAL '2' HOUR                 |
        | backward before midnight          | false | TIME '01:00:00' - INTERVAL '2' HOUR                 |
        | a whole day interval              | true  | TIME '12:00:00' + INTERVAL '2' DAY                  |
        | one microsecond past the last one | true  | TIME '23:59:59.999999' + INTERVAL '0.000001' SECOND |
        | one microsecond before midnight   | true  | TIME '00:00:00' - INTERVAL '0.000001' SECOND        |

    @sail-bug
    Scenario: try_add turns a TIME overflow into NULL
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT try_add(TIME '23:00:00', INTERVAL '2' HOUR) AS overflow, try_add(TIME '10:00:00', INTERVAL '2' HOUR) AS ok
        """
      Then query result
        | overflow | ok       |
        | NULL     | 12:00:00 |

  @spark-4.1
  Rule: TIME minus TIME

    # `SubtractTimes` (Spark 4.2.0 timeExpressions.scala) returns INTERVAL HOUR TO SECOND.
    @sail-bug
    Scenario: TIME column minus TIME column is an hour-to-second interval
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT t - s AS r FROM VALUES
          (TIME '12:00:00', TIME '01:30:00.5'),
          (TIME '00:00:00', TIME '23:59:59.999999'),
          (TIME '09:00:00', TIME '09:00:00'),
          (TIME '09:00:00', NULL)
          AS x(t, s)
        """
      Then query schema
        """
        root
         |-- r: interval hour to second (nullable = true)
        """
      And query result
        | r                                          |
        | INTERVAL '10:29:59.5' HOUR TO SECOND       |
        | INTERVAL '-23:59:59.999999' HOUR TO SECOND |
        | INTERVAL '00:00:00' HOUR TO SECOND         |
        | NULL                                       |

    # The NULL arms of Add and Subtract cast the NULL to a day-time interval (Add) or to the
    # TIME type of the other side (Subtract), so the result types differ.
    @sail-bug
    Scenario: TIME with an untyped NULL operand keeps Spark's result types
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          TIME '12:00:00' + NULL AS time_plus_null,
          NULL + TIME '12:00:00' AS null_plus_time,
          TIME '12:00:00' - NULL AS time_minus_null,
          NULL - TIME '12:00:00' AS null_minus_time
        """
      Then query schema
        """
        root
         |-- time_plus_null: time(6) (nullable = true)
         |-- null_plus_time: time(6) (nullable = true)
         |-- time_minus_null: interval hour to second (nullable = true)
         |-- null_minus_time: interval hour to second (nullable = true)
        """

    # Under ANSI the STRING side is implicitly cast to TIME; without ANSI it becomes DOUBLE.
    @sail-bug
    Scenario: STRING and TIME subtract as TIME values under ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT '01:00:00' - TIME '12:00:00' AS string_first, TIME '12:00:00' - '01:00:00' AS time_first
        """
      Then query result
        | string_first                        | time_first                         |
        | INTERVAL '-11:00:00' HOUR TO SECOND | INTERVAL '11:00:00' HOUR TO SECOND |

    @sail-bug
    Scenario Outline: STRING and TIME subtraction is rejected without ANSI: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS r
        """
      Then query error BINARY_OP_DIFF_TYPES

      Examples:
        | case         | expr                         |
        | string first | '01:00:00' - TIME '12:00:00' |
        | time first   | TIME '12:00:00' - '01:00:00' |

  @spark-4.1
  Rule: TIME arithmetic that Spark rejects at analysis

    @sail-bug
    Scenario Outline: TIME arithmetic is rejected: <case> (ANSI <ansi>)
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS r
        """
      Then query error <error>

      Examples:
        | case                             | ansi  | expr                                 | error                   |
        | date plus time                   | true  | DATE '2024-01-15' + TIME '12:00:00'  | UNEXPECTED_INPUT_TYPE   |
        | time plus date                   | true  | TIME '12:00:00' + DATE '2024-01-15'  | UNEXPECTED_INPUT_TYPE   |
        | date minus time                  | true  | DATE '2024-01-15' - TIME '12:00:00'  | UNEXPECTED_INPUT_TYPE   |
        | time minus date                  | true  | TIME '12:00:00' - DATE '2024-01-15'  | CAST_WITHOUT_SUGGESTION |
        | time plus a year-month interval  | true  | TIME '12:00:00' + INTERVAL '2' MONTH | BINARY_OP_DIFF_TYPES    |
        | time plus a year-month interval  | false | TIME '12:00:00' + INTERVAL '2' MONTH | BINARY_OP_DIFF_TYPES    |
        | time minus a year-month interval | true  | TIME '12:00:00' - INTERVAL '2' MONTH | BINARY_OP_DIFF_TYPES    |
        | a year-month interval plus time  | true  | INTERVAL '2' MONTH + TIME '12:00:00' | BINARY_OP_DIFF_TYPES    |
        | a year-month interval minus time | true  | INTERVAL '1' YEAR - TIME '12:00:00'  | BINARY_OP_DIFF_TYPES    |
        | time plus time                   | true  | TIME '12:00:00' + TIME '01:00:00'    | BINARY_OP_WRONG_TYPE    |
        | time plus time                   | false | TIME '12:00:00' + TIME '01:00:00'    | BINARY_OP_WRONG_TYPE    |
        | time plus a string               | true  | TIME '12:00:00' + '01:00:00'         | BINARY_OP_WRONG_TYPE    |
        | time times a number              | true  | TIME '12:00:00' * 2                  | BINARY_OP_DIFF_TYPES    |
        | time divided by an interval      | true  | TIME '12:00:00' / INTERVAL '1' HOUR  | BINARY_OP_DIFF_TYPES    |
        | negated time                     | true  | -TIME '12:00:00'                     | UNEXPECTED_INPUT_TYPE   |

    @sail-bug
    Scenario Outline: TIME column arithmetic is rejected: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT t + o AS r FROM VALUES (TIME '12:00:00', <o1>), (TIME '01:30:00', <o2>) AS x(t, o)
        """
      Then query error <error>

      Examples:
        | case                         | o1                 | o2                | error                 |
        | a year-month interval column | INTERVAL '2' MONTH | INTERVAL '1' YEAR | BINARY_OP_DIFF_TYPES  |
        | a TIME column                | TIME '01:00:00'    | TIME '02:00:00'   | BINARY_OP_WRONG_TYPE  |
        | a DATE column                | DATE '2024-01-15'  | DATE '2024-02-20' | UNEXPECTED_INPUT_TYPE |

  @spark-4.1
  Rule: CAST from TIME

    Scenario: CAST of a TIME column to STRING drops trailing fractional zeros
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(t AS STRING) AS s FROM VALUES
          (TIME '00:00:00'), (TIME '09:05:03.05'), (TIME '23:59:59.999999'), (NULL) AS x(t)
        """
      Then query result
        | s               |
        | 00:00:00        |
        | 09:05:03.05     |
        | 23:59:59.999999 |
        | NULL            |

    @sail-bug
    Scenario: CAST of a TIME literal to STRING is not nullable
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56.5' AS STRING) AS r
        """
      Then query schema
        """
        root
         |-- r: string (nullable = false)
        """

    # `Cast.castToTime` truncates a TIME to the target precision (`truncateTimeToPrecision`).
    @sail-bug
    Scenario: CAST of TIME to every sub-microsecond precision truncates
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT
          CAST(TIME '12:34:56.987654' AS TIME(1)) AS p1,
          CAST(TIME '12:34:56.987654' AS TIME(2)) AS p2,
          CAST(TIME '12:34:56.987654' AS TIME(4)) AS p4,
          CAST(TIME '12:34:56.987654' AS TIME(5)) AS p5
        """
      Then query schema
        """
        root
         |-- p1: time(1) (nullable = false)
         |-- p2: time(2) (nullable = false)
         |-- p4: time(4) (nullable = false)
         |-- p5: time(5) (nullable = false)
        """
      And query result
        | p1         | p2          | p4            | p5             |
        | 12:34:56.9 | 12:34:56.98 | 12:34:56.9876 | 12:34:56.98765 |

    Scenario: CAST of a TIME column to TIME(0) and TIME(3) truncates
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(t AS TIME(0)) AS p0, CAST(t AS TIME(3)) AS p3 FROM VALUES
          (TIME '00:00:00.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999') AS x(t)
        """
      Then query schema
        """
        root
         |-- p0: time(0) (nullable = false)
         |-- p3: time(3) (nullable = false)
        """
      And query result
        | p0       | p3           |
        | 00:00:00 | 00:00:00.999 |
        | 09:05:03 | 09:05:03.5   |
        | 23:59:59 | 23:59:59.999 |

    @sail-bug
    Scenario: TIME precision beyond 6 is rejected
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56' AS TIME(7)) AS r
        """
      Then query error UNSUPPORTED_TIME_PRECISION

    # TIME to integral is the whole seconds of the day; to decimal keeps the fraction.
    @sail-bug
    Scenario: CAST of a TIME literal to numbers counts seconds since midnight
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CAST(TIME '12:34:56.5' AS BIGINT) AS l,
          CAST(TIME '12:34:56.5' AS INT) AS i,
          CAST(TIME '23:59:59.999999' AS DECIMAL(20, 6)) AS d,
          CAST(TIME '00:00:01' AS SMALLINT) AS s
        """
      Then query result
        | l     | i     | d            | s |
        | 45296 | 45296 | 86399.999999 | 1 |

    # Sail returns microseconds instead of seconds here: a silent wrong value.
    @sail-bug
    Scenario: CAST of a TIME column to BIGINT counts seconds since midnight
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(t AS BIGINT) AS l FROM VALUES
          (TIME '00:00:00'), (TIME '09:05:03.5'), (TIME '23:59:59.999999') AS x(t)
        """
      Then query result
        | l     |
        | 0     |
        | 32703 |
        | 86399 |

    @sail-bug
    Scenario Outline: CAST of TIME to a too narrow number overflows under ANSI: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56' AS <type>) AS r
        """
      Then query error <error>

      Examples:
        | case          | type          | error                      |
        | TINYINT       | TINYINT       | CAST_OVERFLOW              |
        | SMALLINT      | SMALLINT      | CAST_OVERFLOW              |
        | DECIMAL(4, 0) | DECIMAL(4, 0) | NUMERIC_VALUE_OUT_OF_RANGE |

    @sail-bug
    Scenario: CAST of TIME to a too narrow number is NULL without ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(TIME '12:34:56' AS TINYINT) AS t,
          CAST(TIME '12:34:56' AS SMALLINT) AS s,
          CAST(TIME '12:34:56' AS DECIMAL(4, 0)) AS d
        """
      Then query result
        | t    | s    | d    |
        | NULL | NULL | NULL |

    @sail-bug
    Scenario Outline: CAST between TIME and <case> is rejected at analysis
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(<value> AS <type>) AS r
        """
      Then query error CAST_WITHOUT_SUGGESTION

      Examples:
        | case                    | value                               | type                    |
        | TIMESTAMP target        | TIME '12:34:56'                     | TIMESTAMP               |
        | TIMESTAMP_NTZ target    | TIME '12:34:56'                     | TIMESTAMP_NTZ           |
        | DATE target             | TIME '12:34:56'                     | DATE                    |
        | DOUBLE target           | TIME '12:34:56.5'                   | DOUBLE                  |
        | BOOLEAN target          | TIME '12:34:56'                     | BOOLEAN                 |
        | INTERVAL HOUR TO SECOND | TIME '12:34:56'                     | INTERVAL HOUR TO SECOND |
        | TIMESTAMP source        | TIMESTAMP '2024-01-15 12:34:56'     | TIME                    |
        | TIMESTAMP_NTZ source    | TIMESTAMP_NTZ '2024-01-15 12:34:56' | TIME                    |
        | INT source              | 43200                               | TIME                    |

  @spark-4.1
  Rule: CAST from STRING to TIME

    # `DateTimeUtils.stringToTime` trims the input and accepts an optional `T` prefix and
    # one-digit fields.
    @sail-bug
    Scenario: CAST of STRING to TIME accepts Spark's lenient forms
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(s AS TIME) AS r FROM VALUES
          ('00:00:00'), ('9:5:3.5'), (' 23:59:59.999999 '), ('T12:34:56'), (NULL) AS x(s)
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
        | 12:34:56        |
        | NULL            |

    @sail-bug
    Scenario: CAST of a STRING literal to TIME is nullable
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('12:34:56.123' AS TIME(3)) AS r
        """
      Then query schema
        """
        root
         |-- r: time(3) (nullable = true)
        """
      And query result
        | r            |
        | 12:34:56.123 |

    @sail-bug
    Scenario Outline: CAST of a malformed STRING to TIME fails under ANSI: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<value> AS TIME) AS r
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case              | value                 |
        | garbage           | 'garbage'             |
        | the hour 24       | '24:00:00'            |
        | an hour beyond 23 | '25:00:00'            |
        | a date and time   | '2024-01-15 12:34:56' |
        | a bare number     | '12'                  |

    @sail-bug
    Scenario: CAST of a malformed STRING column to TIME is NULL without ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(s AS TIME) AS r FROM VALUES ('10:00:00'), ('garbage'), ('24:00:00'), ('12') AS x(s)
        """
      Then query result
        | r        |
        | 10:00:00 |
        | NULL     |
        | NULL     |
        | NULL     |
