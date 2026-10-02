Feature: timestampadd function

    Scenario Outline: timestampadd: <case>
      When query
        """
        SELECT timestampadd(<unit>, <n>, timestamp<ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | unit        | n  | ts                           | result                     |
        | add years        | YEAR        | 2  | '2016-03-11 09:00:07'        | 2018-03-11 09:00:07        |
        | add weeks        | WEEK        | 5  | '2016-03-11 09:00:07'        | 2016-04-15 09:00:07        |
        | subtract days    | day         | -5 | '2016-03-11 09:00:07'        | 2016-03-06 09:00:07        |
        | add microseconds | MICROSECOND | 2  | '2016-03-11 09:00:07.000001' | 2016-03-11 09:00:07.000003 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null timestamp literal yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a non-null timestamp column yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, CAST(id AS TIMESTAMP)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT timestampadd(HOUR, 1, c) AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: timestampadd through a force-nullable implicit cast: <case>
      When query
        """
        SELECT timestampadd(HOUR, <input>, TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case             | input             |
        | DOUBLE -> BIGINT | CAST(1 AS DOUBLE) |

    Scenario Outline: timestampadd without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT timestampadd(HOUR, <input>, TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |

  # Spark 4.2.0 DateTimeUtils.timestampAdd: MONTH/QUARTER/YEAR go through ZonedDateTime.plusMonths
  # (day clamped to the target month), the other units add a fixed amount; the result may leave
  # 0001..9999 as long as it fits the microsecond Long.
  Rule: timestampadd across calendar edges

    Scenario Outline: timestampadd edge: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampadd(<unit>, <n>, <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | unit        | n     | ts                              | result                 |
        | month clamps Jan 31 to Feb 29        | MONTH       | 1     | TIMESTAMP '2024-01-31 12:00:00' | 2024-02-29 12:00:00    |
        | negative month clamps Mar 31         | MONTH       | -1    | TIMESTAMP '2024-03-31 12:00:00' | 2024-02-29 12:00:00    |
        | quarter clamps Nov 30 to Feb 28      | QUARTER     | 1     | TIMESTAMP '2024-11-30 12:00:00' | 2025-02-28 12:00:00    |
        | year from a leap day clamps          | YEAR        | 1     | TIMESTAMP '2024-02-29 08:00:00' | 2025-02-28 08:00:00    |
        | negative years back to a leap day    | YEAR        | -4    | TIMESTAMP '2024-02-29 08:00:00' | 2020-02-29 08:00:00    |
        | negative weeks across a leap day     | WEEK        | -2    | TIMESTAMP '2024-03-01 00:00:00' | 2024-02-16 00:00:00    |
        | minutes across a leap day            | MINUTE      | 1500  | TIMESTAMP '2024-02-28 23:00:00' | 2024-03-01 00:00:00    |
        | negative seconds across a day        | SECOND      | -86401 | TIMESTAMP '2024-03-01 00:00:00' | 2024-02-28 23:59:59   |
        | milliseconds carry into the next day | MILLISECOND | 1500  | TIMESTAMP '2024-02-28 23:59:59' | 2024-02-29 00:00:00.5  |
        | DAYOFYEAR is a day                   | DAYOFYEAR   | 1     | TIMESTAMP '2024-02-28 23:59:59' | 2024-02-29 23:59:59    |
        | day before the minimum timestamp     | DAY         | -1    | TIMESTAMP '0001-01-01 00:00:00' | 0000-12-31 00:00:00    |
        | day past the maximum timestamp       | DAY         | 1     | TIMESTAMP '9999-12-31 12:00:00' | +10000-01-01 12:00:00  |
        | years past 9999                      | YEAR        | 8000  | TIMESTAMP '2024-01-01 00:00:00' | +10024-01-01 00:00:00  |
        | a DATE is promoted to a timestamp    | DAY         | 1     | DATE '2024-02-28'               | 2024-02-29 00:00:00    |
        | NULL quantity                        | DAY         | CAST(NULL AS INT) | TIMESTAMP '2024-01-01 00:00:00' | NULL     |
        | NULL timestamp                       | DAY         | 1     | CAST(NULL AS TIMESTAMP)         | NULL                   |

    Scenario: timestampadd reads each row's own quantity and timestamp
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT timestampadd(MONTH, n, ts) AS result
        FROM VALUES (1, 1, TIMESTAMP '2024-01-31 10:00:00'), (2, 1, TIMESTAMP '2023-01-31 10:00:00'),
          (3, -1, TIMESTAMP '2024-03-31 10:00:00'), (4, 13, TIMESTAMP '2024-01-31 10:00:00'),
          (5, CAST(NULL AS INT), TIMESTAMP '2024-01-31 10:00:00') AS t(i, n, ts)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2024-02-29 10:00:00 |
        | 2023-02-28 10:00:00 |
        | 2024-02-29 10:00:00 |
        | 2025-02-28 10:00:00 |
        | NULL                |

    # The Long.MaxValue timestamp is +294247-01-10; Spark still returns results up to that bound.
    @sail-bug
    Scenario Outline: timestampadd reaches years far beyond 9999: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | expr                                                                | result                 |
        | many days                    | timestampadd(DAY, 100000000, TIMESTAMP '2024-01-01 00:00:00')       | +275814-09-14 00:00:00 |
        | many seconds                 | timestampadd(SECOND, 9000000000000, TIMESTAMP '2024-01-01 00:00:00') | +287222-08-24 16:00:00 |
        | timestamp plus year interval | TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '290000' YEAR            | +292024-01-01 00:00:00 |

    # timestampAdd wraps every ArithmeticException/DateTimeException (multiplyExact, toIntExact,
    # instantToMicros) into DATETIME_OVERFLOW, whatever spark.sql.ansi.enabled says.
    @sail-bug
    Scenario Outline: timestampadd overflow raises DATETIME_OVERFLOW under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT timestampadd(<unit>, <n>, TIMESTAMP '2024-01-01 00:00:00') AS result
        """
      Then query error \[DATETIME_OVERFLOW\]

      Examples:
        | case                                  | ansi  | unit        | n                   |
        | microseconds overflow the Long        | true  | MICROSECOND | 9223372036854775807 |
        | microseconds overflow the Long        | false | MICROSECOND | 9223372036854775807 |
        | milliseconds overflow multiplyExact   | true  | MILLISECOND | 9223372036854775807 |
        | milliseconds overflow multiplyExact   | false | MILLISECOND | 9223372036854775807 |
        | hours overflow multiplyExact          | true  | HOUR        | 9223372036854775807 |
        | seconds past the Long timestamp       | true  | SECOND      | 9300000000000       |
        | days beyond INT                       | true  | DAY         | 2147483648          |
        | days beyond INT                       | false | DAY         | 2147483648          |
        | Int.MaxValue days                     | true  | DAY         | 2147483647          |
        | weeks past the Long timestamp         | true  | WEEK        | 400000000           |
        | months beyond INT                     | true  | MONTH       | 2147483648          |
        | years past the Long timestamp         | true  | YEAR        | 300000              |
        | years past the Long timestamp         | false | YEAR        | 300000              |
        | years overflow the month count        | true  | YEAR        | 200000000           |

    @sail-bug
    Scenario: timestampadd overflow in one row of a column raises DATETIME_OVERFLOW
      When query
        """
        SELECT timestampadd(MILLISECOND, n, TIMESTAMP '2024-01-01 00:00:00') AS result FROM VALUES (1500L), (9223372036854775807L) AS t(n)
        """
      Then query error \[DATETIME_OVERFLOW\]

    # TimestampAdd.dataType = timestamp.dataType, so a TIMESTAMP_NTZ input stays TIMESTAMP_NTZ.
    @sail-bug
    Scenario: timestampadd keeps the TIMESTAMP_NTZ type
      When query
        """
        SELECT timestampadd(MONTH, 1, TIMESTAMP_NTZ '2024-01-31 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit string casts

    @sail-bug
    Scenario Outline: timestampadd through a string cast is nullable: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT timestampadd(HOUR, <n>, <ts>) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case                | n   | ts                              |
        | string quantity     | '2' | TIMESTAMP '2024-01-01 00:00:00' |
        | string timestamp    | 1   | '2024-01-01 00:00:00'           |

  # Spark 4.2.0 DateTimeUtils.timestampAdd: DAY/WEEK/MONTH/QUARTER/YEAR go through
  # ZonedDateTime.plusDays/plusMonths in the session zone (local wall clock kept, a DST gap shifts
  # forward), while HOUR and smaller add elapsed microseconds. TIMESTAMP_NTZ uses UTC, i.e. no zone.
  Rule: timestampadd in the session time zone

    @sail-bug
    Scenario Outline: timestampadd calendar units keep the local wall clock: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT timestampadd(<unit>, <n>, TIMESTAMP '<ts>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | zone                | unit    | n | ts                  | result              |
        | LA day across the spring-forward gap  | America/Los_Angeles | DAY     | 1 | 2024-03-09 12:00:00 | 2024-03-10 12:00:00 |
        | LA week across the spring-forward gap | America/Los_Angeles | WEEK    | 1 | 2024-03-09 12:00:00 | 2024-03-16 12:00:00 |
        | LA day across the fall-back overlap   | America/Los_Angeles | DAY     | 1 | 2024-11-02 12:00:00 | 2024-11-03 12:00:00 |
        | Chatham day across its fall-back      | Pacific/Chatham     | DAY     | 1 | 2024-04-06 12:00:00 | 2024-04-07 12:00:00 |
        | Chatham day across its spring-forward | Pacific/Chatham     | DAY     | 1 | 2024-09-28 12:00:00 | 2024-09-29 12:00:00 |
        | LA month into the gap shifts forward  | America/Los_Angeles | MONTH   | 1 | 2024-02-10 02:30:00 | 2024-03-10 03:30:00 |
        | LA quarter into the gap               | America/Los_Angeles | QUARTER | 1 | 2023-12-10 02:30:00 | 2024-03-10 03:30:00 |
        | LA year into the gap                  | America/Los_Angeles | YEAR    | 1 | 2023-03-10 02:30:00 | 2024-03-10 03:30:00 |

    Scenario Outline: timestampadd outside UTC without a wall-clock divergence: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT timestampadd(<unit>, <n>, TIMESTAMP '<ts>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | zone                | unit        | n          | ts                  | result              |
        | LA 24 hours across the gap is elapsed  | America/Los_Angeles | HOUR        | 24         | 2024-03-09 12:00:00 | 2024-03-10 13:00:00 |
        | LA 24 hours across the overlap         | America/Los_Angeles | HOUR        | 24         | 2024-11-02 12:00:00 | 2024-11-03 11:00:00 |
        | LA 1440 minutes across the overlap     | America/Los_Angeles | MINUTE      | 1440       | 2024-11-02 12:00:00 | 2024-11-03 11:00:00 |
        | LA an hour of seconds in the overlap   | America/Los_Angeles | SECOND      | 3600       | 2024-11-03 01:30:00 | 2024-11-03 01:30:00 |
        | LA an hour of milliseconds over gap    | America/Los_Angeles | MILLISECOND | 3600000    | 2024-03-10 01:30:00 | 2024-03-10 03:30:00 |
        | LA an hour of microseconds over gap    | America/Los_Angeles | MICROSECOND | 3600000000 | 2024-03-10 01:30:00 | 2024-03-10 03:30:00 |
        | LA day landing in the gap              | America/Los_Angeles | DAY         | 1          | 2024-03-09 02:30:00 | 2024-03-10 03:30:00 |
        | Chatham 24 hours across its fall-back  | Pacific/Chatham     | HOUR        | 24         | 2024-04-06 12:00:00 | 2024-04-07 11:00:00 |
        | Chatham 24 hours across spring-forward | Pacific/Chatham     | HOUR        | 24         | 2024-09-28 12:00:00 | 2024-09-29 13:00:00 |
        | Kolkata day at a late local hour       | Asia/Kolkata        | DAY         | 1          | 2024-01-31 23:00:00 | 2024-02-01 23:00:00 |
        | Kolkata month at a late local hour     | Asia/Kolkata        | MONTH       | 1          | 2024-01-31 23:00:00 | 2024-02-29 23:00:00 |
        | Kolkata hour into the next day         | Asia/Kolkata        | HOUR        | 1          | 2024-01-31 23:00:00 | 2024-02-01 00:00:00 |
        | Pago Pago month on local Jan 31        | Pacific/Pago_Pago   | MONTH       | 1          | 2024-01-31 20:00:00 | 2024-02-29 20:00:00 |
        | Pago Pago day onto a local leap day    | Pacific/Pago_Pago   | DAY         | 1          | 2024-02-28 20:00:00 | 2024-02-29 20:00:00 |
        | Pago Pago year from a local leap day   | Pacific/Pago_Pago   | YEAR        | 1          | 2024-02-29 20:00:00 | 2025-02-28 20:00:00 |

    @sail-bug
    Scenario: timestampadd DAY keeps each row's local wall clock in Los Angeles
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT timestampadd(DAY, n, ts) AS result
        FROM VALUES (1, 1, TIMESTAMP '2024-03-09 12:00:00'), (2, 1, TIMESTAMP '2024-11-02 12:00:00'),
          (3, 2, TIMESTAMP '2024-06-01 12:00:00') AS t(i, n, ts)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2024-03-10 12:00:00 |
        | 2024-11-03 12:00:00 |
        | 2024-06-03 12:00:00 |

    @sail-bug
    Scenario Outline: timestampadd on TIMESTAMP_NTZ ignores the session zone: <case>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT timestampadd(<unit>, <n>, TIMESTAMP_NTZ '<ts>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | unit  | n  | ts                  | result              |
        | 24 hours over LA's gap stay 24 hours   | HOUR  | 24 | 2024-03-09 12:00:00 | 2024-03-10 12:00:00 |
        | a day over LA's gap                    | DAY   | 1  | 2024-03-09 12:00:00 | 2024-03-10 12:00:00 |
        | a month onto LA's missing hour         | MONTH | 1  | 2024-02-10 02:30:00 | 2024-03-10 02:30:00 |
        | a day onto LA's missing hour           | DAY   | 1  | 2024-03-09 02:30:00 | 2024-03-10 02:30:00 |

  # Spark 4.2.0 DateTimeUtils.timestampAddDayTime: a day-time interval is split into whole days
  # (added on the local calendar) and the remaining microseconds; timestampAddInterval does the
  # same for months and days of a calendar interval.
  Rule: TIMESTAMP plus an interval in the session time zone

    @sail-bug
    Scenario Outline: timestamp plus a day-time interval adds whole days on the local calendar: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT TIMESTAMP '<ts>' <op> <interval> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | zone                | ts                  | op | interval                                | result              |
        | LA one day over the gap               | America/Los_Angeles | 2024-03-09 12:00:00 | +  | INTERVAL '1' DAY                        | 2024-03-10 12:00:00 |
        | LA 24 hours over the gap              | America/Los_Angeles | 2024-03-09 12:00:00 | +  | INTERVAL '24' HOUR                      | 2024-03-10 12:00:00 |
        | LA day and six hours over the gap     | America/Los_Angeles | 2024-03-09 12:00:00 | +  | INTERVAL '1 06:00:00' DAY TO SECOND     | 2024-03-10 18:00:00 |
        | LA 30 hours over the gap              | America/Los_Angeles | 2024-03-09 12:00:00 | +  | INTERVAL '30' HOUR                      | 2024-03-10 18:00:00 |
        | LA 1440 minutes over the gap          | America/Los_Angeles | 2024-03-09 12:00:00 | +  | INTERVAL '1440' MINUTE                  | 2024-03-10 12:00:00 |
        | LA one day over the overlap           | America/Los_Angeles | 2024-11-02 12:00:00 | +  | INTERVAL '1' DAY                        | 2024-11-03 12:00:00 |
        | LA 24 hours over the overlap          | America/Los_Angeles | 2024-11-02 12:00:00 | +  | INTERVAL '24' HOUR                      | 2024-11-03 12:00:00 |
        | LA one day back over the gap          | America/Los_Angeles | 2024-03-10 12:00:00 | -  | INTERVAL '1' DAY                        | 2024-03-09 12:00:00 |
        | LA 24 hours back over the gap         | America/Los_Angeles | 2024-03-10 12:00:00 | -  | INTERVAL '24' HOUR                      | 2024-03-09 12:00:00 |
        | Chatham one day over its fall-back    | Pacific/Chatham     | 2024-04-06 12:00:00 | +  | INTERVAL '1' DAY                        | 2024-04-07 12:00:00 |
        | Chatham 24 hours over its fall-back   | Pacific/Chatham     | 2024-04-06 12:00:00 | +  | INTERVAL '24' HOUR                      | 2024-04-07 12:00:00 |
        | LA month into the gap shifts forward  | America/Los_Angeles | 2024-02-10 02:30:00 | +  | INTERVAL '1' MONTH                      | 2024-03-10 03:30:00 |

    @sail-bug
    Scenario: timestamp plus day-time intervals keeps each row's local wall clock in Los Angeles
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT ts + INTERVAL '24' HOUR AS hours, ts + INTERVAL '1' DAY AS days
        FROM VALUES (1, TIMESTAMP '2024-03-09 12:00:00'), (2, TIMESTAMP '2024-11-02 12:00:00'),
          (3, TIMESTAMP '2024-06-01 12:00:00') AS t(i, ts)
        ORDER BY i
        """
      Then query result ordered
        | hours               | days                |
        | 2024-03-10 12:00:00 | 2024-03-10 12:00:00 |
        | 2024-11-03 12:00:00 | 2024-11-03 12:00:00 |
        | 2024-06-02 12:00:00 | 2024-06-02 12:00:00 |

    Scenario Outline: timestamp plus an interval outside UTC without a divergence: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <ts> <op> <interval> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | zone                | ts                                  | op | interval                             | result              |
        | LA calendar-interval day over the gap    | America/Los_Angeles | TIMESTAMP '2024-03-09 12:00:00'     | +  | make_interval(0, 0, 0, 1, 0, 0, 0)   | 2024-03-10 12:00:00 |
        | LA calendar-interval 24 hours is elapsed | America/Los_Angeles | TIMESTAMP '2024-03-09 12:00:00'     | +  | make_interval(0, 0, 0, 0, 24, 0, 0)  | 2024-03-10 13:00:00 |
        | LA day landing in the gap                | America/Los_Angeles | TIMESTAMP '2024-03-09 02:30:00'     | +  | INTERVAL '1' DAY                     | 2024-03-10 03:30:00 |
        | LA hour inside the overlap               | America/Los_Angeles | TIMESTAMP '2024-11-03 01:30:00'     | +  | INTERVAL '1' HOUR                    | 2024-11-03 01:30:00 |
        | LA NTZ 24 hours over the gap             | America/Los_Angeles | TIMESTAMP_NTZ '2024-03-09 12:00:00' | +  | INTERVAL '24' HOUR                   | 2024-03-10 12:00:00 |
        | LA NTZ month onto the missing hour       | America/Los_Angeles | TIMESTAMP_NTZ '2024-02-10 02:30:00' | +  | INTERVAL '1' MONTH                   | 2024-03-10 02:30:00 |
        | Kolkata month at a late local hour       | Asia/Kolkata        | TIMESTAMP '2024-01-31 23:00:00'     | +  | INTERVAL '1' MONTH                   | 2024-02-29 23:00:00 |
        | Kolkata day and two hours                | Asia/Kolkata        | TIMESTAMP '2024-01-31 23:00:00'     | +  | INTERVAL '1 02:00:00' DAY TO SECOND  | 2024-02-02 01:00:00 |
        | Pago Pago month on local Jan 31          | Pacific/Pago_Pago   | TIMESTAMP '2024-01-31 20:00:00'     | +  | INTERVAL '1' MONTH                   | 2024-02-29 20:00:00 |
        | Pago Pago year back from a leap day      | Pacific/Pago_Pago   | TIMESTAMP '2024-02-29 20:00:00'     | -  | INTERVAL '1' YEAR                    | 2023-02-28 20:00:00 |
        | Chatham month on local Jan 31            | Pacific/Chatham     | TIMESTAMP '2024-01-31 00:30:00'     | +  | INTERVAL '1' MONTH                   | 2024-02-29 00:30:00 |

    Scenario: timestamp plus a year-month interval column clamps each row in Chatham
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT ts + i AS result
        FROM VALUES (1, TIMESTAMP '2024-01-31 00:30:00', INTERVAL '1' MONTH), (2, TIMESTAMP '2024-03-31 00:30:00', INTERVAL '1' MONTH),
          (3, TIMESTAMP '2024-02-29 00:30:00', INTERVAL '-12' MONTH) AS t(n, ts, i)
        ORDER BY n
        """
      Then query result ordered
        | result              |
        | 2024-02-29 00:30:00 |
        | 2024-04-30 00:30:00 |
        | 2023-02-28 00:30:00 |
