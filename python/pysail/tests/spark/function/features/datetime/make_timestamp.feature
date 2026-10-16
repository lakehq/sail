Feature: make_timestamp_ntz and try_make_timestamp_ntz functions

  Rule: Basic timestamp creation with 6 arguments

    Scenario Outline: Six arguments: <case>
      When query
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | args                            | result                     |
        | create timestamp from date and time components | 2014, 12, 28, 6, 30, 45.887     | 2014-12-28 06:30:45.887    |
        | create timestamp at midnight                   | 2023, 12, 31, 0, 0, 0.0         | 2023-12-31 00:00:00        |
        | create timestamp near end of valid range       | 9999, 12, 31, 23, 58, 59.999999 | 9999-12-31 23:58:59.999999 |
        | sec=60 adds one minute                         | 2024, 6, 15, 14, 30, 60.0       | 2024-06-15 14:31:00        |

  Rule: Timestamp creation with date and time arguments

    Scenario Outline: Date and time: <case>
      When query
        """
        SELECT make_timestamp_ntz(DATE <date>, TIME <time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | date         | time              | result                     |
        | combine date and time                            | '2024-03-15' | '14:30:00'        | 2024-03-15 14:30:00        |
        | combine date and time with microsecond precision | '2024-01-01' | '12:34:56.123456' | 2024-01-01 12:34:56.123456 |

  Rule: try_make_timestamp_ntz with valid inputs

    Scenario: valid 6-argument call
      When query
        """
        SELECT try_make_timestamp_ntz(2024, 2, 14, 15, 45, 30.5) AS result
        """
      Then query result
        | result                |
        | 2024-02-14 15:45:30.5 |

    Scenario: valid date and time combination
      When query
        """
        SELECT try_make_timestamp_ntz(DATE '2024-07-04', TIME '18:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-07-04 18:00:00 |

  Rule: try_make_timestamp_ntz with invalid inputs returns NULL

    Scenario Outline: Invalid component: <case>
      When query
        """
        SELECT try_make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case           | args                   |
        | invalid month  | 2024, 13, 1, 0, 0, 0.0 |
        | invalid day    | 2024, 2, 30, 0, 0, 0.0 |
        | invalid hour   | 2024, 1, 1, 24, 0, 0.0 |
        | invalid minute | 2024, 1, 1, 0, 60, 0.0 |
        | invalid second | 2024, 1, 1, 0, 0, 61.0 |

  Rule: NULL handling

    Scenario: make_timestamp_ntz with null date
      When query
        """
        SELECT make_timestamp_ntz(CAST(NULL AS DATE), TIME '10:00:00') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_make_timestamp_ntz with null year
      When query
        """
        SELECT try_make_timestamp_ntz(CAST(NULL AS INT), 1, 1, 0, 0, 0.0) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multiple rows with mixed valid and invalid inputs

    Scenario: try_make_timestamp_ntz on array of inputs
      When query
        """
        SELECT
          year,
          try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2024, 1, 1, 0, 0, 0.0),
          (2024, 13, 1, 0, 0, 0.0),
          (2024, 6, 15, 12, 30, 45.5)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year, month, day
        """
      Then query result ordered
        | year | result                |
        | 2024 | 2024-01-01 00:00:00   |
        | 2024 | 2024-06-15 12:30:45.5 |
        | 2024 | NULL                  |

  Rule: Per-element NULL propagation with 6 arguments over columns

    Scenario: try_make_timestamp_ntz null second only, rest valid, returns NULL not a valid timestamp
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null second only over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null year over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, 1, 1, 0, 0, 0.0) AS result
        FROM VALUES (2020), (CAST(NULL AS INT))
        AS t(year)
        ORDER BY year NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: try_make_timestamp_ntz any null component over columns returns NULL
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (CAST(NULL AS INT), 1, 1, 0, 0, 0.0),
          (2020, CAST(NULL AS INT), 1, 0, 0, 0.0),
          (2020, 1, CAST(NULL AS INT), 0, 0, 0.0),
          (2020, 1, 1, CAST(NULL AS INT), 0, 0.0),
          (2020, 1, 1, 0, CAST(NULL AS INT), 0.0),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE)),
          (2020, 1, 1, 0, 0, 0.0)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year NULLS FIRST, month NULLS FIRST, day NULLS FIRST, hour NULLS FIRST, min NULLS FIRST, sec NULLS FIRST
        """
      Then query result ordered
        | result              |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | 2020-01-01 00:00:00 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: non-null components yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    @sail-bug
    Scenario: non-null component columns yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable component column stays nullable
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, c) AS result FROM VALUES (CAST(0 AS INT)), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: make_timestamp without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT make_timestamp(<input>, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 2024  |

    Scenario Outline: make_timestamp through a force-nullable implicit cast: <case>
      When query
        """
        SELECT make_timestamp(<input>, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case          | input  |
        | STRING -> INT | '2024' |

  # Spark 4.2.0 datetimeExpressions.scala, MakeTimestamp.toMicros: `LocalDateTime.of(...)`
  # with sec cast to DECIMAL(16, 6). A `DateTimeException` becomes
  # `ansiDateTimeArgumentOutOfRange` when failOnError (= ANSI) and NULL otherwise;
  # sec = 60 with a fraction raises INVALID_FRACTION_OF_SECOND.
  Rule: make_timestamp components are validated by java.time and honour ANSI mode

    @sail-bug
    Scenario Outline: make_timestamp rejects an invalid component under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp(<args>) AS result
        """
      Then query error \[DATETIME_FIELD_OUT_OF_BOUNDS

      Examples:
        | case                      | args                 |
        | month 13                  | 2024, 13, 1, 0, 0, 0 |
        | leap day on non-leap year | 2023, 2, 29, 0, 0, 0 |
        | hour 25                   | 2024, 1, 1, 25, 0, 0 |
        | minute 60                 | 2024, 1, 1, 0, 60, 0 |
        | second 61                 | 2024, 1, 1, 0, 0, 61 |
        | negative second           | 2024, 1, 1, 0, 0, -1 |

    @sail-bug
    Scenario: make_timestamp rejects a fractional second 60 under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 60.5) AS result
        """
      Then query error \[INVALID_FRACTION_OF_SECOND

    @sail-bug
    Scenario: make_timestamp rejects a second that does not fit DECIMAL(16, 6) under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 10000000000) AS result
        """
      Then query error \[NUMERIC_VALUE_OUT_OF_RANGE

    @sail-bug
    Scenario Outline: <function> returns NULL for an invalid component without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <function>(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | function           | case                         | args                          |
        | make_timestamp     | month 13                     | 2024, 13, 1, 0, 0, 0          |
        | make_timestamp     | leap day on non-leap year    | 2023, 2, 29, 0, 0, 0          |
        | make_timestamp     | hour 25                      | 2024, 1, 1, 25, 0, 0          |
        | make_timestamp     | second 61                    | 2024, 1, 1, 0, 0, 61          |
        | make_timestamp     | fractional second 60         | 2024, 1, 1, 0, 0, 60.5        |
        | make_timestamp     | second beyond DECIMAL(16, 6) | 2024, 1, 1, 0, 0, 10000000000 |
        | make_timestamp_ntz | month 13                     | 2024, 13, 1, 0, 0, 0          |

    # Row by row: one invalid row must become NULL, not fail the whole batch.
    @sail-bug
    Scenario: make_timestamp resolves invalid rows to NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp(y, mo, d, h, mi, s) AS result
        FROM VALUES
          (1, 2024, 2, 29, 23, 59, 59.5),
          (2, 2023, 2, 29, 0, 0, 0.0),
          (3, 2024, 1, 1, 24, 0, 0.0),
          (4, 1999, 7, 4, 12, 30, 60.0)
        AS t(i, y, mo, d, h, mi, s)
        ORDER BY i
        """
      Then query result ordered
        | result                |
        | 2024-02-29 23:59:59.5 |
        | NULL                  |
        | NULL                  |
        | 1999-07-04 12:31:00   |

    # The sec argument is cast to DECIMAL(16, 6), which rounds half-up.
    Scenario Outline: make_timestamp rounds the second to microseconds: <case>
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, <sec>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | sec         | result                     |
        | nine fractional digits         | 1.123456789 | 2024-01-01 00:00:01.123457 |
        | rounds up into the next minute | 59.9999995  | 2024-01-01 00:01:00        |

  # java.time's LocalDateTime accepts any year in -999999999..999999999; Spark's
  # TIMESTAMP only has to fit a microsecond Long (about -290308..294247).
  Rule: make_timestamp builds timestamps outside 0001..9999

    @sail-bug
    Scenario Outline: <function> builds a timestamp for <case>
      When query
        """
        SELECT <function>(<year>, 1, 1, 0, 0, 0) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function           | case            | year  | result                |
        | make_timestamp     | year zero       | 0     | 0000-01-01 00:00:00   |
        | make_timestamp     | negative year   | -1    | -0001-01-01 00:00:00  |
        | make_timestamp     | five-digit year | 10000 | +10000-01-01 00:00:00 |
        | make_timestamp_ntz | year zero       | 0     | 0000-01-01 00:00:00   |
        | make_timestamp_ntz | five-digit year | 10000 | +10000-01-01 00:00:00 |

    @sail-bug
    Scenario: make_timestamp builds out-of-range years from a column
      When query
        """
        SELECT make_timestamp(y, 1, 1, 0, 0, 0) AS result
        FROM VALUES (1, 2024), (2, 10000), (3, 0) AS t(i, y)
        ORDER BY i
        """
      Then query result ordered
        | result                |
        | 2024-01-01 00:00:00   |
        | +10000-01-01 00:00:00 |
        | 0000-01-01 00:00:00   |

  # Spark 4.2.0 DateTimeUtils.getZoneId accepts every java.time ZoneId form: region IDs,
  # 'Z', '+HH', '+HH:mm' and prefixed offsets such as 'GMT+2' or 'UTC+01:00'.
  Rule: make_timestamp accepts zone offsets as the time zone

    @sail-bug
    Scenario Outline: make_timestamp interprets the fields in the zone <tz>
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 0, '<tz>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | tz        | result              |
        | Z         | 2024-01-01 00:00:00 |
        | +05:30    | 2023-12-31 18:30:00 |
        | -08:00    | 2024-01-01 08:00:00 |
        | +01       | 2023-12-31 23:00:00 |
        | GMT+2     | 2023-12-31 22:00:00 |
        | UTC+01:00 | 2023-12-31 23:00:00 |

    @sail-bug
    Scenario: make_timestamp takes zone offsets from a column
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 0, tz) AS result
        FROM VALUES (1, '+05:30'), (2, '-08:00'), (3, 'Asia/Tokyo') AS t(i, tz)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2023-12-31 18:30:00 |
        | 2024-01-01 08:00:00 |
        | 2023-12-31 15:00:00 |

    Scenario: make_timestamp takes region time zones from a column
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 0, tz) AS result
        FROM VALUES (1, 'UTC'), (2, 'Asia/Tokyo'), (3, 'America/New_York') AS t(i, tz)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2024-01-01 00:00:00 |
        | 2023-12-31 15:00:00 |
        | 2024-01-01 05:00:00 |

  Rule: make_timestamp_ntz arity

    # Spark 4.2.0 MakeTimestampNTZExpressionBuilder accepts 2 or 6 arguments only.
    @sail-bug
    Scenario: make_timestamp_ntz rejects a time zone argument
      When query
        """
        SELECT make_timestamp_ntz(2024, 1, 1, 0, 0, 0, 'UTC') AS result
        """
      Then query error \[WRONG_NUM_ARGS

  # Spark 4.2.0 datetimeExpressions.scala, MakeTimestamp.toMicros:
  # `ldt.atZone(zoneId).toInstant`, where zoneId is the session time zone unless a
  # timezone argument is given. java.time resolves a local time in a DST gap by shifting
  # it forward by the gap, and one in an overlap to the EARLIER offset.
  Rule: make_timestamp interprets the fields in the session time zone

    Scenario Outline: make_timestamp resolves winter and summer fields in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          unix_seconds(make_timestamp(2024, 1, 15, 10, 0, 0)) AS winter,
          unix_seconds(make_timestamp(2024, 7, 15, 10, 0, 0)) AS summer,
          make_timestamp(2024, 1, 1, 0, 0, 0, 'UTC') AS utc_midnight
        """
      Then query result
        | winter   | summer   | utc_midnight   |
        | <winter> | <summer> | <utc_midnight> |

      Examples:
        | zone                | winter     | summer     | utc_midnight        |
        | America/Los_Angeles | 1705341600 | 1721062800 | 2023-12-31 16:00:00 |
        | Asia/Kolkata        | 1705293000 | 1721017800 | 2024-01-01 05:30:00 |
        | Pacific/Chatham     | 1705263300 | 1720991700 | 2024-01-01 13:45:00 |
        | Pacific/Pago_Pago   | 1705352400 | 1721077200 | 2023-12-31 13:00:00 |

    Scenario Outline: make_timestamp resolves column fields in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(make_timestamp(y, 1, 15, h, 0, 0)) AS result
        FROM VALUES (1, 2024, 10), (2, 2023, 23), (3, 1999, 0) AS t(i, y, h)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1         | r2         | r3        |
        | America/Los_Angeles | 1705341600 | 1673852400 | 916387200 |
        | Asia/Kolkata        | 1705293000 | 1673803800 | 916338600 |
        | Pacific/Chatham     | 1705263300 | 1673774100 | 916308900 |
        | Pacific/Pago_Pago   | 1705352400 | 1673863200 | 916398000 |

    Scenario Outline: make_timestamp renders a column time zone in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT make_timestamp(2024, 1, 1, 0, 0, 0, tz) AS result
        FROM VALUES (1, 'UTC'), (2, 'Asia/Kolkata'), (3, 'Pacific/Chatham') AS t(i, tz)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1                  | r2                  | r3                  |
        | America/Los_Angeles | 2023-12-31 16:00:00 | 2023-12-31 10:30:00 | 2023-12-31 02:15:00 |
        | Pacific/Pago_Pago   | 2023-12-31 13:00:00 | 2023-12-31 07:30:00 | 2023-12-30 23:15:00 |

    @sail-bug
    Scenario Outline: make_timestamp resolves a DST transition in the session zone: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(make_timestamp(<fields>)) AS epoch, make_timestamp(<fields>) AS result
        """
      Then query result
        | epoch   | result   |
        | <epoch> | <result> |

      Examples:
        | case                               | zone                | fields                | epoch      | result              |
        | Los Angeles gap moves forward      | America/Los_Angeles | 2024, 3, 10, 2, 30, 0 | 1710066600 | 2024-03-10 03:30:00 |
        | Los Angeles overlap takes earlier  | America/Los_Angeles | 2024, 11, 3, 1, 30, 0 | 1730622600 | 2024-11-03 01:30:00 |
        | Chatham gap moves forward          | Pacific/Chatham     | 2024, 9, 29, 3, 0, 0  | 1727532900 | 2024-09-29 04:00:00 |
        | Chatham overlap takes earlier      | Pacific/Chatham     | 2024, 4, 7, 3, 0, 0   | 1712409300 | 2024-04-07 03:00:00 |

    @sail-bug
    Scenario: make_timestamp resolves DST transition rows in the session zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_seconds(make_timestamp(y, mo, d, h, mi, 0)) AS result
        FROM VALUES (1, 2024, 3, 10, 2, 30), (2, 2024, 11, 3, 1, 30), (3, 2024, 3, 10, 3, 30) AS t(i, y, mo, d, h, mi)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1710066600 |
        | 1730622600 |
        | 1710066600 |

    # Paired with the DST gap above: the same fields with an explicit zone argument, and
    # the TIMESTAMP_NTZ variant, which has no zone to resolve.
    Scenario: make_timestamp with an explicit zone moves a DST gap forward
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          make_timestamp(2024, 3, 10, 2, 30, 0, 'America/Los_Angeles') AS ltz,
          make_timestamp_ntz(2024, 3, 10, 2, 30, 0) AS ntz
        """
      Then query result
        | ltz                 | ntz                 |
        | 2024-03-10 03:30:00 | 2024-03-10 02:30:00 |
