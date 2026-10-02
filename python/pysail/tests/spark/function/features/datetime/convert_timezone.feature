Feature: convert_timezone

  Rule: Type coercion

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` with coercible input
      When query
        """
        SELECT convert_timezone('America/Los_Angeles', 'Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 23:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 23:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 23:30:00 |
        | DATE '2024-06-15'                     | 2024-06-15 09:00:00 |

    Scenario Outline: `convert_timezone` with coercible input and implicit source timezone
      When query
        """
        SELECT convert_timezone('Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 08:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 08:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 08:30:00 |
        | DATE '2024-06-15'                     | 2024-06-14 18:00:00 |

    Scenario Outline: uncastable null time zones are rejected
      When query
        """
        SELECT convert_timezone(
          <source>,
          <target>,
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        )
        """
      Then query error (?i)(DATATYPE_MISMATCH\.(UNEXPECTED_INPUT_TYPE|CAST_WITHOUT_SUGGESTION)|time zone arguments must be castable to string|cannot cast .* to VARIANT)

      Examples:
        | source                           | target                                           |
        | CAST(NULL AS STRUCT<value: INT>) | 'UTC'                                            |
        | CAST(NULL AS STRING)             | CAST(NULL AS MAP<STRING, INT>)                   |
        | CAST(NULL AS STRING)             | CAST(CAST(NULL AS STRUCT<value: INT>) AS VARIANT) |
        | CAST(NULL AS STRING)             | CAST(CAST(NULL AS MAP<STRING, INT>) AS VARIANT)   |

    @sail-bug
    Scenario: an ARRAY time zone fails analysis even when it is NULL
      When query
        """
        SELECT convert_timezone(
          CAST(NULL AS ARRAY<STRING>),
          'UTC',
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        """
      Then query error DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

  Rule: Null propagation

    @sail-bug
    Scenario: a direct NULL suppresses a Python UDF before extraction
      Given scalar Python UDF fail_string raises direct-null-target-evaluated
      When query
        """
        SELECT convert_timezone(
          CAST(NULL AS STRING),
          fail_string(id),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a direct NULL suppresses a valid Variant time zone expression
      When query
        """
        SELECT convert_timezone(
          CAST(NULL AS STRING),
          CAST(CAST(1 AS INT) AS VARIANT),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a raw NULL stops later expression evaluation
      When query
        """
        SELECT convert_timezone(
          NULL,
          CAST(raise_error('raw-null-zone') AS STRING),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: scalar time zones are not parsed when another argument is null
      When query
        """
        SELECT
          convert_timezone(
            CAST(NULL AS STRING),
            'Not/AZone',
            TIMESTAMP_NTZ '2024-01-01 00:00:00'
          ) AS null_source,
          convert_timezone(
            'Not/AZone',
            CAST(NULL AS STRING),
            TIMESTAMP_NTZ '2024-01-01 00:00:00'
          ) AS null_target,
          convert_timezone(
            'Not/AZone',
            'UTC',
            CAST(NULL AS TIMESTAMP_NTZ)
          ) AS null_timestamp
        """
      Then query result
        | null_source | null_target | null_timestamp |
        | NULL        | NULL        | NULL           |

    @sail-bug
    Scenario: an arbitrary foldable NULL suppresses an earlier row-dependent error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT convert_timezone(
          CAST(1 / id AS STRING),
          concat(CAST(NULL AS STRING), 'UTC'),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a foldable NULL does not suppress an earlier foldable error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT convert_timezone(
          CAST(1 / 0 AS STRING),
          IF(true, CAST(NULL AS STRING), 'UTC'),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        """
      Then query error (?i)(DIVIDE_BY_ZERO|divide by zero)

    Scenario: per-row time zones are not parsed when another value is null
      When query
        """
        SELECT
          label,
          convert_timezone(source_tz, target_tz, source_ts) AS result
        FROM VALUES
          ('null_source', CAST(NULL AS STRING), 'Not/AZone',
            TIMESTAMP_NTZ '2024-01-01 00:00:00'),
          ('null_target', 'Not/AZone', CAST(NULL AS STRING),
            TIMESTAMP_NTZ '2024-01-01 00:00:00'),
          ('null_timestamp', 'Not/AZone', 'UTC',
            CAST(NULL AS TIMESTAMP_NTZ)),
          ('valid', 'UTC', 'UTC',
            TIMESTAMP_NTZ '2024-01-01 00:00:00')
          AS t(label, source_tz, target_tz, source_ts)
        ORDER BY label
        """
      Then query result ordered
        | label          | result              |
        | null_source    | NULL                |
        | null_target    | NULL                |
        | null_timestamp | NULL                |
        | valid          | 2024-01-01 00:00:00 |

    Scenario: expressions after a null argument are not evaluated
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT convert_timezone(
          CASE
            WHEN id = 0 THEN CAST(NULL AS STRING)
            ELSE 'UTC'
          END,
          IF(id = 0, CAST(1 / id AS STRING), 'UTC'),
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        FROM range(0, 2, 1, 1)
        ORDER BY id
        """
      Then query result ordered
        | result              |
        | NULL                |
        | 2024-01-01 00:00:00 |

    @sail-bug
    Scenario: errors follow Spark's row-major evaluation order
      When query
        """
        SELECT convert_timezone(
          CASE WHEN id = 0 THEN 'Not/AZone' ELSE 'UTC' END,
          CASE WHEN id = 1 THEN raise_error('later-row') ELSE 'UTC' END,
          TIMESTAMP_NTZ '2024-01-01 00:00:00'
        ) AS result
        FROM VALUES (0), (1) AS t(id)
        """
      Then query error (INVALID_TIMEZONE|Unknown time-zone ID).*Not/AZone

    Scenario: the lazy wrapper does not shadow a surrounding lambda parameter
      When query
        """
        SELECT transform(
          array('UTC'),
          _convert_tz -> convert_timezone(
            _convert_tz,
            'UTC',
            TIMESTAMP_NTZ '2024-01-01 00:00:00'
          )
        ) AS result
        """
      Then query result
        | result                |
        | [2024-01-01 00:00:00] |

  Rule: Wide timestamp range

    @sail-bug
    Scenario: `convert_timezone` preserves timestamps outside Chrono's range
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT convert_timezone(
          'UTC',
          'America/Los_Angeles',
          CAST(timestamp_micros(9000000000000000000) AS TIMESTAMP_NTZ)
        ) IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` around daylight saving time transition
      When query
        """
        SELECT convert_timezone(<from>, <to>, <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | from                  | to                    | result              |
        | '2025-03-09 01:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 10:30:00 |
        | '2025-03-09 02:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 03:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 10:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 01:30:00 |
        | '2025-03-09 11:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 03:30:00 |

    Scenario: `convert_timezone` resolves a nonexistent local time when the zones match
      When query
        """
        SELECT convert_timezone(
          'America/Los_Angeles',
          'America/Los_Angeles',
          TIMESTAMP_NTZ '2021-03-14 02:30:00'
        ) AS result
        """
      Then query result
        | result              |
        | 2021-03-14 03:30:00 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to convert_timezone yields the schema Spark declares
      When query
        """
        SELECT convert_timezone('Europe/Brussels', 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    Scenario: a non-null column input to convert_timezone yields the schema Spark declares
      When query
        """
        SELECT convert_timezone(CAST(id AS STRING), 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    Scenario: a nullable column input to convert_timezone stays nullable
      When query
        """
        SELECT convert_timezone(c, 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result FROM VALUES ('Europe/Brussels'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

  Rule: Time zone ids follow Spark's getZoneId

    # Spark 4.2.0 DateTimeUtils.convertTimestampNtzToAnotherTz calls SparkDateTimeUtils.getZoneId
    # for both zones: ZoneId.of(id, ZoneId.SHORT_IDS), so offsets are valid, and an invalid id
    # raises INVALID_TIMEZONE whatever the ANSI mode.

    Background:
      Given config spark.sql.session.timeZone = UTC

    @sail-bug
    Scenario Outline: `convert_timezone` accepts fixed-offset time zone ids: <case>
      When query
        """
        SELECT convert_timezone('<source>', '<target>', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | source | target | result              |
        | from +14:00 to -12:00             | +14:00 | -12:00 | 2023-12-31 10:00:00 |
        | to a single-digit hour offset     | UTC    | +8:00  | 2024-01-01 20:00:00 |
        | between prefixed offsets          | GMT+8  | Z      | 2024-01-01 04:00:00 |

    @sail-bug
    Scenario: `convert_timezone` resolves a fixed-offset source zone per row
      When query
        """
        SELECT id, convert_timezone(s, 'UTC', TIMESTAMP_NTZ '2024-06-01 12:00:00') AS result
        FROM VALUES (1, 'Asia/Tokyo'), (2, '+05:45'), (3, 'Z'), (4, 'GMT+8') AS t(id, s)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 2024-06-01 03:00:00 |
        | 2  | 2024-06-01 06:15:00 |
        | 3  | 2024-06-01 12:00:00 |
        | 4  | 2024-06-01 04:00:00 |

    Scenario Outline: `convert_timezone` rejects an invalid time zone id under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT convert_timezone('<source>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS result
        """
      Then query error INVALID_TIMEZONE

      Examples:
        | case                   | ansi  | source    |
        | unknown region         | false | Not/AZone |
        | UTC in lowercase       | true  | utc       |

  Rule: Results may leave the 0001-9999 range

    # java.time's LocalDateTime range is much wider than 0001-9999, and historical instants use
    # the zone's local mean time (Pacific/Kiritimati is -10:29:20 in year 1).

    Scenario: `convert_timezone` crosses both ends of the 0001-9999 range
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          convert_timezone('Etc/GMT+12', 'Pacific/Kiritimati', TIMESTAMP_NTZ '9999-12-31 12:00:00') AS past_max,
          convert_timezone('Pacific/Kiritimati', 'Etc/GMT+12', TIMESTAMP_NTZ '0001-01-01 12:00:00') AS near_min
        """
      Then query result
        | past_max              | near_min            |
        | +10000-01-01 14:00:00 | 0001-01-01 10:29:20 |

    Scenario: `convert_timezone` of distinct boundary rows from a column
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, convert_timezone('Asia/Tokyo', 'America/New_York', ts) AS result
        FROM VALUES
          (1, TIMESTAMP_NTZ '0001-01-01 12:00:00'),
          (2, TIMESTAMP_NTZ '9999-12-31 12:00:00'),
          (3, TIMESTAMP_NTZ '1582-10-15 05:00:00')
          AS t(id, ts)
        ORDER BY id
        """
      Then query result ordered
        | id | result              |
        | 1  | 0000-12-31 21:44:59 |
        | 2  | 9999-12-30 22:00:00 |
        | 3  | 1582-10-14 14:44:59 |

  Rule: A local time in the fall-back overlap takes the earlier offset

    Scenario: `convert_timezone` from a zone whose local time happens twice
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          convert_timezone('America/Los_Angeles', 'UTC', TIMESTAMP_NTZ '2024-11-03 01:30:00') AS overlap,
          convert_timezone('America/Los_Angeles', 'UTC', TIMESTAMP_NTZ '2024-03-10 02:30:00') AS gap
        """
      Then query result
        | overlap             | gap                 |
        | 2024-11-03 08:30:00 | 2024-03-10 10:30:00 |
