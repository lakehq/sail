Feature: to_utc_timestamp

  Rule: Type coercion

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `to_utc_timestamp` with coercible input
      When query
        """
        SELECT to_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 21:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 21:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 21:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 21:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 21:30:00 |
        | '2024-06-15 13:30:00+07:00'           | 2024-06-15 21:30:00 |
        | DATE '2024-06-15'                     | 2024-06-15 07:00:00 |

  Rule: Invalid time zone validation

    Scenario: `to_utc_timestamp` validates a constant zone for a runtime-null timestamp
      When query
        """
        SELECT to_utc_timestamp(
          CASE
            WHEN id = 0 THEN CAST(NULL AS TIMESTAMP)
            ELSE TIMESTAMP '2024-01-01 00:00:00'
          END,
          'Not/AZone'
        ) AS result
        FROM range(0, 1, 1, 1)
        """
      Then query error INVALID_TIMEZONE

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `to_utc_timestamp` around daylight saving time transition
      When query
        """
        SELECT to_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | result              |
        | '2025-03-09 09:30:00' | 2025-03-09 17:30:00 |
        | '2025-03-09 10:30:00' | 2025-03-09 18:30:00 |
        | '2025-03-09 11:30:00' | 2025-03-09 18:30:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT to_utc_timestamp('2016-08-31', 'Asia/Seoul') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null column input to to_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT to_utc_timestamp(CAST(id AS STRING), 'Asia/Seoul') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable column input to to_utc_timestamp stays nullable
      When query
        """
        SELECT to_utc_timestamp(c, 'Asia/Seoul') AS result FROM VALUES ('2016-08-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Time zone ids follow Spark's getZoneId

    # Spark 4.2.0 DateTimeUtils.toUTCTime -> SparkDateTimeUtils.getZoneId: ZoneId.of(id,
    # ZoneId.SHORT_IDS), so bare offsets and prefixed offsets are valid zones, not just region ids.

    Background:
      Given config spark.sql.session.timeZone = UTC

    @sail-bug
    Scenario Outline: `to_utc_timestamp` accepts a fixed-offset time zone id: <case>
      When query
        """
        SELECT to_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', '<tz>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | tz     | result              |
        | maximum usual offset +14:00 | +14:00 | 2023-12-31 10:00:00 |
        | minimum usual offset -12:00 | -12:00 | 2024-01-01 12:00:00 |
        | GMT-prefixed offset         | GMT+8  | 2023-12-31 16:00:00 |

    Scenario: `to_utc_timestamp` resolves a region time zone per row
      When query
        """
        SELECT id, to_utc_timestamp(ts, tz) AS result
        FROM VALUES
          (1, TIMESTAMP '2024-06-01 12:00:00', 'Asia/Tokyo'),
          (2, TIMESTAMP '2024-06-01 12:00:00', 'America/New_York'),
          (3, TIMESTAMP '2024-06-01 12:00:00', CAST(NULL AS STRING)),
          (4, CAST(NULL AS TIMESTAMP), 'UTC')
          AS t(id, ts, tz)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 2024-06-01 03:00:00 |
        | 2  | 2024-06-01 16:00:00 |
        | 3  | NULL                |
        | 4  | NULL                |

    @sail-bug
    Scenario: `to_utc_timestamp` resolves a fixed-offset time zone per row
      When query
        """
        SELECT id, to_utc_timestamp(ts, tz) AS result
        FROM VALUES
          (1, TIMESTAMP '2024-06-01 12:00:00', 'Asia/Tokyo'),
          (2, TIMESTAMP '2024-06-01 12:00:00', '+05:45')
          AS t(id, ts, tz)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 2024-06-01 03:00:00 |
        | 2  | 2024-06-01 06:15:00 |

    Scenario: `to_utc_timestamp` rejects an invalid time zone id under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', 'Not/AZone') AS result
        """
      Then query error INVALID_TIMEZONE

  Rule: The fall-back overlap resolves to the earlier offset

    # Spark 4.2.0 DateTimeUtils.convertTz goes through ZonedDateTime.of(LocalDateTime, zone), which
    # picks the earlier offset (PDT, -07:00) for a local time that happens twice.

    Scenario: `to_utc_timestamp` inside the fall-back overlap
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          to_utc_timestamp(TIMESTAMP '2024-11-03 01:30:00', 'America/Los_Angeles') AS overlap,
          to_utc_timestamp(TIMESTAMP '2024-11-03 02:30:00', 'America/Los_Angeles') AS after
        """
      Then query result
        | overlap             | after               |
        | 2024-11-03 08:30:00 | 2024-11-03 10:30:00 |

  Rule: Results may leave the 0001-9999 range

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_utc_timestamp` shifts past the end of year 9999 and uses local mean time
      When query
        """
        SELECT id, to_utc_timestamp(ts, 'America/New_York') AS result
        FROM VALUES
          (1, TIMESTAMP '0001-01-01 00:00:00'),
          (2, TIMESTAMP '9999-12-31 22:00:00'),
          (3, TIMESTAMP '1582-10-15 00:00:00')
          AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | id | result                |
        | 1  | 0001-01-01 04:56:02   |
        | 2  | +10000-01-01 03:00:00 |
        | 3  | 1582-10-15 04:56:02   |

  Rule: A TIMESTAMP_NTZ input is localized in the session time zone

    # Spark 4.2.0 UTCTimestamp.inputTypes is (TimestampType, StringType): the TIMESTAMP_NTZ is cast
    # to TIMESTAMP in the session zone first (gap forward, overlap to the earlier offset).

    @sail-bug
    Scenario: `to_utc_timestamp` of a timestamp_ntz inside the spring-forward gap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT to_utc_timestamp(TIMESTAMP_NTZ '2024-03-10 02:30:00', 'Asia/Tokyo') AS result
        """
      Then query result
        | result              |
        | 2024-03-09 17:30:00 |

    @sail-bug
    Scenario: `to_utc_timestamp` of a timestamp_ntz inside the fall-back overlap keeps the earlier instant
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_timestamp(to_utc_timestamp(TIMESTAMP_NTZ '2024-11-03 01:30:00', 'UTC')) AS result
        """
      Then query result
        | result     |
        | 1730622600 |
