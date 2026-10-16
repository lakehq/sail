Feature: from_utc_timestamp

  Rule: Type coercion

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `from_utc_timestamp` with coercible input
      When query
        """
        SELECT from_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 07:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 07:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 07:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 07:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 07:30:00 |
        | '2024-06-15 13:30:00+07:00'           | 2024-06-15 07:30:00 |
        | DATE '2024-06-15'                     | 2024-06-14 17:00:00 |

  Rule: Invalid time zone validation

    Scenario: `from_utc_timestamp` validates a constant zone for a runtime-null timestamp
      When query
        """
        SELECT from_utc_timestamp(
          CASE
            WHEN id = 0 THEN CAST(NULL AS TIMESTAMP)
            ELSE TIMESTAMP '2024-01-01 00:00:00'
          END,
          'Not/AZone'
        ) AS result
        FROM range(0, 1, 1, 1)
        """
      Then query error INVALID_TIMEZONE

    @sail-bug
    Scenario: `from_utc_timestamp` skips a dynamic zone for a null timestamp
      When query
        """
        SELECT from_utc_timestamp(source_ts, target_tz) AS result
        FROM VALUES
          (CAST(NULL AS TIMESTAMP), 'Not/AZone')
          AS t(source_ts, target_tz)
        """
      Then query result
        | result |
        | NULL   |

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `from_utc_timestamp` around daylight saving time transition
      When query
        """
        SELECT from_utc_timestamp(<ts>, 'America/Los_Angeles') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | result              |
        | '2025-03-09 17:30:00' | 2025-03-09 09:30:00 |
        | '2025-03-09 18:30:00' | 2025-03-09 11:30:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to from_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null column input to from_utc_timestamp yields the schema Spark declares
      When query
        """
        SELECT from_utc_timestamp(CAST(id AS STRING), 'Asia/Seoul') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable column input to from_utc_timestamp stays nullable
      When query
        """
        SELECT from_utc_timestamp(c, 'Asia/Seoul') AS result FROM VALUES ('2016-08-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Time zone ids follow Spark's getZoneId

    # Spark 4.2.0 DateTimeUtils.fromUTCTime -> SparkDateTimeUtils.getZoneId: ZoneId.of(id,
    # ZoneId.SHORT_IDS) after rewriting `(+|-)h:mm` and `(+|-)hh:m`. So region ids, the SHORT_IDS
    # aliases, bare offsets, `Z` and prefixed offsets (`GMT+8`, `UTC+05:30`, `UT+3`) are all valid.

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: `from_utc_timestamp` accepts a region or short time zone id: <case>
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', '<tz>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | tz         | result              |
        | short id PST                | PST        | 2023-12-31 16:00:00 |
        | short id IST                | IST        | 2024-01-01 05:30:00 |
        | short id EST is fixed -5    | EST        | 2023-12-31 19:00:00 |
        | Etc zone with inverted sign | Etc/GMT-14 | 2024-01-01 14:00:00 |

    @sail-bug
    Scenario Outline: `from_utc_timestamp` accepts a fixed-offset time zone id: <case>
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', '<tz>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | tz        | result              |
        | maximum usual offset +14:00   | +14:00    | 2024-01-01 14:00:00 |
        | minimum usual offset -12:00   | -12:00    | 2023-12-31 12:00:00 |
        | quarter-hour offset +05:45    | +05:45    | 2024-01-01 05:45:00 |
        | hour-only offset -08          | -08       | 2023-12-31 16:00:00 |
        | offset with seconds           | +05:30:15 | 2024-01-01 05:30:15 |
        | single-digit hour offset      | +8:00     | 2024-01-01 08:00:00 |
        | single-digit minute offset    | +08:5     | 2024-01-01 08:05:00 |
        | GMT-prefixed offset           | GMT+8     | 2024-01-01 08:00:00 |
        | UTC-prefixed offset           | UTC+05:30 | 2024-01-01 05:30:00 |
        | UT-prefixed offset            | UT+3      | 2024-01-01 03:00:00 |
        | Z                             | Z         | 2024-01-01 00:00:00 |

    Scenario Outline: `from_utc_timestamp` rejects an invalid time zone id under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP '2024-01-01 00:00:00', '<tz>') AS result
        """
      Then query error INVALID_TIMEZONE

      Examples:
        | case                         | ansi  | tz                  |
        | offset beyond +18:00         | true  | +18:01              |
        | offset beyond +18:00         | false | +18:01              |
        | region id in lowercase       | true  | america/los_angeles |
        | UTC in lowercase             | false | utc                 |
        | empty id                     | true  |                     |

    Scenario: `from_utc_timestamp` resolves a region time zone per row
      When query
        """
        SELECT id, from_utc_timestamp(ts, tz) AS result
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
        | 1  | 2024-06-01 21:00:00 |
        | 2  | 2024-06-01 08:00:00 |
        | 3  | NULL                |
        | 4  | NULL                |

    @sail-bug
    Scenario: `from_utc_timestamp` resolves a fixed-offset time zone per row
      When query
        """
        SELECT id, from_utc_timestamp(ts, tz) AS result
        FROM VALUES
          (1, TIMESTAMP '2024-06-01 12:00:00', 'Asia/Tokyo'),
          (2, TIMESTAMP '2024-06-01 12:00:00', '+05:45'),
          (3, TIMESTAMP '2024-06-01 12:00:00', 'GMT-3')
          AS t(id, ts, tz)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 2024-06-01 21:00:00 |
        | 2  | 2024-06-01 17:45:00 |
        | 3  | 2024-06-01 09:00:00 |

  Rule: Results may leave the 0001-9999 range

    # Spark 4.2.0 DateTimeUtils.convertTz shifts the microseconds with java.time, whose range is far
    # wider than 0001-9999, so a shift across either end is returned rather than rejected. Old
    # instants use the zone's local mean time (Asia/Tokyo is +09:18:59 before 1888).

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `from_utc_timestamp` shifts past the end of year 9999
      When query
        """
        SELECT
          from_utc_timestamp(TIMESTAMP '9999-12-31 23:00:00', 'Pacific/Kiritimati') AS past_max,
          from_utc_timestamp(TIMESTAMP '0001-01-01 01:00:00', 'Etc/GMT+12') AS before_min
        """
      Then query result
        | past_max              | before_min          |
        | +10000-01-01 13:00:00 | 0000-12-31 13:00:00 |

    Scenario: `from_utc_timestamp` uses local mean time for historical instants
      When query
        """
        SELECT id, from_utc_timestamp(ts, 'Asia/Tokyo') AS result
        FROM VALUES
          (1, TIMESTAMP '0001-01-01 00:00:00'),
          (2, TIMESTAMP '9999-12-31 00:00:00'),
          (3, TIMESTAMP '1582-10-04 20:00:00')
          AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 0001-01-01 09:18:59 |
        | 2  | 9999-12-31 09:00:00 |
        | 3  | 1582-10-05 05:18:59 |

  Rule: A TIMESTAMP_NTZ input is localized in the session time zone

    # Spark 4.2.0 UTCTimestamp.inputTypes is (TimestampType, StringType): a TIMESTAMP_NTZ is first
    # cast to TIMESTAMP in the session zone, which resolves a DST gap forward and an overlap to the
    # earlier offset.

    @sail-bug
    Scenario: `from_utc_timestamp` of a timestamp_ntz inside the spring-forward gap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP_NTZ '2024-03-10 02:30:00', 'UTC') AS result
        """
      Then query result
        | result              |
        | 2024-03-10 03:30:00 |

    @sail-bug
    Scenario: `from_utc_timestamp` of a timestamp_ntz inside the fall-back overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT from_utc_timestamp(TIMESTAMP_NTZ '2024-11-03 01:30:00', 'Asia/Tokyo') AS result
        """
      Then query result
        | result              |
        | 2024-11-03 09:30:00 |

    @sail-bug
    Scenario: `from_utc_timestamp` of a timestamp_ntz column across the spring-forward gap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT id, from_utc_timestamp(ts, 'UTC') AS result
        FROM VALUES
          (1, TIMESTAMP_NTZ '2024-03-10 01:30:00'),
          (2, TIMESTAMP_NTZ '2024-03-10 02:30:00')
          AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 2024-03-10 01:30:00 |
        | 2  | 2024-03-10 03:30:00 |
