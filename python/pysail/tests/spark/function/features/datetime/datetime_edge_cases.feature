# Moved from features/datetime_edge_cases.feature by the datetime/ layout reorganisation.
Feature: datetime edge cases

  Rule: 2-digit year expansion boundaries

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: 2-digit year: <case>
      When query
        """
        SELECT to_date('<in>', 'yy-MM-dd') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | in       | result     |
        | `to_date` expands 2-digit year 00 to 2000 | 00-01-01 | 2000-01-01 |
        | `to_date` expands 2-digit year 49 to 2049 | 49-12-31 | 2049-12-31 |
        | `to_date` expands 2-digit year 50 to 2050 | 50-01-01 | 2050-01-01 |
        | `to_date` expands 2-digit year 99 to 2099 | 99-06-15 | 2099-06-15 |

  Rule: Extreme timezone offsets

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Extreme offset date_format: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45<offset>', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | offset | result               |
        | `date_format` handles maximum positive offset +14:00 | +14:00 | 2026-06-15 00:30:45Z |
        | `date_format` handles maximum negative offset -12:00 | -12:00 | 2026-06-16 02:30:45Z |

    Scenario Outline: Extreme offset to_timestamp: <case>
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45<offset>', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | offset | result              |
        | `to_timestamp` parses maximum positive offset +14:00 | +14:00 | 2026-06-15 00:30:45 |
        | `to_timestamp` parses maximum negative offset -12:00 | -12:00 | 2026-06-16 02:30:45 |

  Rule: Half-hour and quarter-hour timezone offsets

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` handles India timezone +05:30
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45+05:30', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result               |
        | 2026-06-15 09:00:45Z |

    Scenario Outline: Sub-hour offset to_timestamp: <case>
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45<offset>', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | offset | result              |
        | `to_timestamp` parses Newfoundland timezone -03:30    | -03:30 | 2026-06-15 18:00:45 |
        | `to_timestamp` parses Nepal timezone +05:45           | +05:45 | 2026-06-15 08:45:45 |
        | `to_timestamp` parses Chatham Islands timezone +12:45 | +12:45 | 2026-06-15 01:45:45 |

  Rule: Clock hour edge cases

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Strict clock field H/HH rejects hour 24: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 24:30:45', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                             | fmt                 |
        | `to_timestamp` rejects H=24 in ANSI mode         | yyyy-MM-dd H:mm:ss  |
        | `to_timestamp` rejects HH=24 in ANSI mode        | yyyy-MM-dd HH:mm:ss |

    Scenario Outline: Strict clock field k/kk maps hour 24 to same-day midnight: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 24:30:45', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | fmt                 | result              |
        | `to_timestamp` maps k=24 to same-day midnight          | yyyy-MM-dd k:mm:ss  | 2026-06-15 00:30:45 |
        | `to_timestamp` maps kk=24 to same-day midnight         | yyyy-MM-dd kk:mm:ss | 2026-06-15 00:30:45 |

    Scenario Outline: 12-hour clock: <case>
      When query
        """
        SELECT to_timestamp('<in>', 'yyyy-MM-dd hh:mm:ss a') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | in                     | result              |
        | `to_timestamp` parses 12-hour midnight with AM | 2026-06-15 12:00:00 AM | 2026-06-15 00:00:00 |
        | `to_timestamp` parses 12-hour noon with PM     | 2026-06-15 12:00:00 PM | 2026-06-15 12:00:00 |

    Scenario: Strict clock field try_to_timestamp returns NULL for invalid hour and second
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          try_to_timestamp('2026-06-15 24:30:45', 'yyyy-MM-dd H:mm:ss') AS hour_24,
          try_to_timestamp('2026-06-15 23:59:60', 'yyyy-MM-dd HH:mm:ss') AS second_60
        """
      Then query result
        | hour_24 | second_60 |
        | NULL    | NULL      |

  Rule: Fractional seconds precision

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Fractional seconds date_format: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '<ts>', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | ts                            | fmt                           | result                        |
        | `date_format` pads fractional seconds to requested width | 2026-06-15 14:30:45.123       | SSSSSSSSS                     | 123000000                     |
        | `date_format` formats minimum nanosecond value           | 2026-06-15 14:30:45.000000001 | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45.000000000 |
        | `date_format` formats maximum nanosecond value           | 2026-06-15 14:30:45.999999999 | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45.999999000 |

    Scenario: `to_timestamp` truncates nanoseconds to microseconds
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                     |
        | 2026-06-15 14:30:45.123456 |

  Rule: Leap second handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: Strict clock field to_timestamp rejects second 60
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 23:59:60', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

  Rule: Era handling with BC dates

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats AD era
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', 'G yyyy-MM-dd') AS result
        """
      Then query result
        | result        |
        | AD 2026-06-15 |

    Scenario: `to_timestamp` parses AD era
      When query
        """
        SELECT to_timestamp('AD 2026-06-15 14:30:45', 'G yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

  Rule: Week-based fields

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` rejects week-of-month pattern W
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'W')
        """
      Then query error .*

    Scenario Outline: Week-based field: <case>
      When query
        """
        SELECT date_format(DATE '2026-06-15', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                        | fmt | result |
        | `date_format` formats aligned week-of-month | F   | 1      |
        | `date_format` formats quarter               | Q   | 2      |
        | `date_format` formats quarter with text     | QQQ | Q2     |

  Rule: Optional sections

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Optional section formatting with fractional seconds: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '<ts>', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                               | ts                         | result                     |
        | `date_format` includes optional section when fraction is zero     | 2026-06-15 14:30:45        | 2026-06-15 14:30:45.000000 |
        | `date_format` includes optional section when fraction is non-zero | 2026-06-15 14:30:45.123456 | 2026-06-15 14:30:45.123456 |

    Scenario: Optional section formatting includes all-zero time fields
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 00:00:00', 'yyyy-MM-dd[ HH:mm:ss]') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 00:00:00 |

    Scenario Outline: Optional section to_timestamp: <case>
      When query
        """
        SELECT to_timestamp('<in>', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | in                         | result                     |
        | `to_timestamp` parses without optional section | 2026-06-15 14:30:45        | 2026-06-15 14:30:45        |
        | `to_timestamp` parses with optional section    | 2026-06-15 14:30:45.123456 | 2026-06-15 14:30:45.123456 |

  Rule: Spark session timezone identifiers

    Scenario: timestamp literals use the Spark timezone parser
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT unix_micros(
          TIMESTAMP '1970-01-01 00:00:00'
        ) AS result
        """
      Then query result
        | result      |
        | -3723000000 |

    Scenario Outline: timestamp results remain usable with Spark session zone IDs
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          CAST(to_timestamp('1970-01-01 00:00:00') AS STRING) AS rendered,
          unix_micros(to_timestamp('1970-01-01 00:00:00')) AS micros
        """
      Then query result
        | rendered            | micros   |
        | 1970-01-01 00:00:00 | <micros> |

      Examples:
        | zone      | micros       |
        | +8        | -28800000000 |
        | GMT+8:30  | -30600000000 |
        | +01:02:03 | -3723000000  |

    @sail-bug
    Scenario: temporal kernels accept second-precision session offsets
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          hour(to_timestamp('1970-01-01 00:00:00')) AS local_hour,
          CAST(to_timestamp('1970-01-01 00:00:00') AS DATE) AS local_date
        """
      Then query result
        | local_hour | local_date |
        | 0          | 1970-01-01 |

    Scenario Outline: timestamp reader accepts a Spark-only time zone ID: <case>
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT unix_micros(
          <function>(<input>, 'ts TIMESTAMP', <options>).ts
        ) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | function  | input                                         | options                                       | result       |
        | CSV option zone   | from_csv  | '1970-01-01 00:00:00'                        | map('timeZone', 'GMT+8:30')                   | -30600000000 |
        | CSV session zone  | from_csv  | '1970-01-01 00:00:00'                        | map('timestampFormat', 'yyyy-MM-dd HH:mm:ss') | -3723000000  |
        | JSON session zone | from_json | '{"ts":"1970-01-01 00:00:00"}'               | map('timestampFormat', 'yyyy-MM-dd HH:mm:ss') | -3723000000  |
        | XML session zone  | from_xml  | '<p><ts>1970-01-01 00:00:00</ts></p>'        | map('timestampFormat', 'yyyy-MM-dd HH:mm:ss') | -3723000000  |

    Scenario Outline: timestamp formatter accepts a second-precision session offset: <case>
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result              |
        | 1970-01-01 01:02:03 |

      Examples:
        | case        | expression                                                                                                                                 |
        | date_format | date_format(to_timestamp('1970-01-01 00:00:00Z'), 'yyyy-MM-dd HH:mm:ss')                                                                |
        | CSV writer  | to_csv(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss'))                         |
        | JSON writer | get_json_object(to_json(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss')), '$.ts') |
        | XML writer  | xpath_string(to_xml(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'yyyy-MM-dd HH:mm:ss')), '/ROW/ts') |

    Scenario Outline: timestamp formatters emit Spark's canonical zone ID
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          date_format(to_timestamp('1970-01-01 00:00:00Z'), 'VV') AS formatted,
          to_csv(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'VV')) AS csv,
          get_json_object(to_json(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'VV')), '$.ts') AS json,
          xpath_string(to_xml(named_struct('ts', to_timestamp('1970-01-01 00:00:00Z')), map('timestampFormat', 'VV')), '/ROW/ts') AS xml
        """
      Then query result
        | formatted | csv        | json       | xml        |
        | <expected> | <expected> | <expected> | <expected> |

      Examples:
        | zone      | expected                     |
        | EST       | America/Panama               |
        | HST       | Pacific/Honolulu             |
        | IET       | America/Indiana/Indianapolis |
        | IST       | Asia/Kolkata                 |
        | MST       | America/Phoenix              |
        | PST       | America/Los_Angeles          |
        | VST       | Asia/Ho_Chi_Minh             |
        | GMT+8:30  | GMT+08:30                    |
        | +8        | +08:00                       |
        | UTC+0     | UTC                          |

  Rule: Local timestamp resolution across time-zone transitions

    Scenario Outline: nonexistent local timestamp moves forward: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          CAST(to_timestamp('<input>') AS STRING) AS rendered,
          unix_micros(to_timestamp('<input>')) AS micros
        """
      Then query result
        | rendered   | micros   |
        | <rendered> | <micros> |

      Examples:
        | case                    | zone                | input               | rendered            | micros           |
        | 30-minute gap           | Australia/Lord_Howe | 2024-10-06 02:15:00 | 2024-10-06 02:45:00 | 1728143100000000 |
        | 44-minute-30-second gap | Africa/Monrovia     | 1972-01-07 00:15:00 | 1972-01-07 00:59:30 | 63593970000000   |
        | full skipped local date | Pacific/Apia        | 2011-12-30 12:00:00 | 2011-12-31 12:00:00 | 1325282400000000 |

    Scenario: an ambiguous local timestamp uses the earlier offset
      Given config spark.sql.session.timeZone = Australia/Lord_Howe
      When query
        """
        SELECT
          CAST(to_timestamp('2024-04-07 01:45:00') AS STRING) AS rendered,
          unix_micros(to_timestamp('2024-04-07 01:45:00')) AS micros
        """
      Then query result
        | rendered            | micros           |
        | 2024-04-07 01:45:00 | 1712414700000000 |

    Scenario Outline: UTC conversion functions resolve TIMESTAMP_NTZ in the session zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_micros(<function>(TIMESTAMP_NTZ '<timestamp>', 'UTC')) AS result
        """
      Then query result
        | result   |
        | <micros> |

      Examples:
        | function           | timestamp           | micros           |
        | from_utc_timestamp | 2025-03-09 02:30:00 | 1741516200000000 |
        | to_utc_timestamp   | 2025-03-09 02:30:00 | 1741516200000000 |
        | from_utc_timestamp | 2025-11-02 01:30:00 | 1762072200000000 |
        | to_utc_timestamp   | 2025-11-02 01:30:00 | 1762072200000000 |

    Scenario: UTC conversion functions coerce NTZ with a second-precision session offset
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(from_utc_timestamp(TIMESTAMP_NTZ '1970-01-01 00:00:00', 'UTC')) AS from_result,
          unix_micros(to_utc_timestamp(TIMESTAMP_NTZ '1970-01-01 00:00:00', 'UTC')) AS to_result
        """
      Then query result
        | from_result | to_result   |
        | -3723000000 | -3723000000 |

    Scenario: CAST and TRY_CAST use Spark gap and overlap resolution
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_micros(CAST(TIMESTAMP_NTZ '2025-03-09 02:30:00' AS TIMESTAMP)) AS cast_gap,
          unix_micros(TRY_CAST(TIMESTAMP_NTZ '2025-03-09 02:30:00' AS TIMESTAMP)) AS try_gap,
          unix_micros(CAST(TIMESTAMP_NTZ '2025-11-02 01:30:00' AS TIMESTAMP)) AS cast_overlap,
          unix_micros(TRY_CAST(TIMESTAMP_NTZ '2025-11-02 01:30:00' AS TIMESTAMP)) AS try_overlap
        """
      Then query result
        | cast_gap         | try_gap          | cast_overlap     | try_overlap      |
        | 1741516200000000 | 1741516200000000 | 1762072200000000 | 1762072200000000 |

    Scenario: time-zone-sensitive casts support Spark IDs and nested types
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(CAST(TIMESTAMP_NTZ '1970-01-01 00:00:00' AS TIMESTAMP)) AS ntz_cast,
          unix_micros(TRY_CAST(TIMESTAMP_NTZ '1970-01-01 00:00:00' AS TIMESTAMP)) AS ntz_try,
          unix_micros(CAST(DATE '1970-01-01' AS TIMESTAMP)) AS date_cast,
          CAST(TIMESTAMP '1970-01-01 00:00:00Z' AS TIMESTAMP_NTZ) AS reverse_cast,
          CAST(array(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ARRAY<TIMESTAMP>) AS nested_timestamps,
          transform(CAST(array(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ARRAY<TIMESTAMP>), x -> unix_micros(x)) AS nested_array,
          unix_micros(element_at(CAST(map('x', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS MAP<STRING,TIMESTAMP>), 'x')) AS nested_map,
          unix_micros(CAST(named_struct('x', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS STRUCT<x:TIMESTAMP>).x) AS nested_struct
        """
      Then query result
        | ntz_cast   | ntz_try    | date_cast  | reverse_cast        | nested_timestamps     | nested_array   | nested_map  | nested_struct |
        | -3723000000 | -3723000000 | -3723000000 | 1970-01-01 01:02:03 | [1970-01-01 00:00:00] | [-3723000000] | -3723000000 | -3723000000   |

    Scenario Outline: relative special datetime values use the session-local date
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          CAST('today' AS DATE) = CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE) AS today,
          CAST('tomorrow' AS DATE) = date_add(CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE), 1) AS tomorrow,
          CAST('yesterday' AS DATE) = date_add(CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE), -1) AS yesterday
        """
      Then query result
        | today | tomorrow | yesterday |
        | true  | true     | true      |

      Examples:
        | zone               |
        | Pacific/Kiritimati |
        | Etc/GMT+12         |

    Scenario Outline: localtimestamp supports Spark session time-zone IDs: <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT localtimestamp() = CAST(current_timestamp() AS TIMESTAMP_NTZ) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | zone      |
        | +01:02:03 |
        | GMT+8:30  |
        | +8        |
        | PST       |
        | UTC+8     |
        | +013045   |
        | IST       |
        | America/Los_Angeles |

    Scenario: lossless TRY_CAST timezone conversions preserve non-nullability
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          TRY_CAST(TIMESTAMP_NTZ '2025-01-01 00:00:00' AS TIMESTAMP) AS ntz_to_ltz,
          TRY_CAST(DATE '2025-01-01' AS TIMESTAMP) AS date_to_ltz,
          TRY_CAST(TIMESTAMP '2025-01-01 00:00:00Z' AS TIMESTAMP_NTZ) AS ltz_to_ntz
        """
      Then query schema
        """
        root
         |-- ntz_to_ltz: timestamp (nullable = false)
         |-- date_to_ltz: timestamp (nullable = false)
         |-- ltz_to_ntz: timestamp_ntz (nullable = false)
        """

    Scenario: folded epoch casts retain Spark's nullable schema
      When query
        """
        SELECT
          CAST('epoch' AS DATE) AS date_cast,
          TRY_CAST('epoch' AS DATE) AS date_try,
          CAST('epoch' AS TIMESTAMP) AS timestamp_cast,
          TRY_CAST('epoch' AS TIMESTAMP) AS timestamp_try,
          CAST('epoch' AS TIMESTAMP_NTZ) AS ntz_cast,
          TRY_CAST('epoch' AS TIMESTAMP_NTZ) AS ntz_try
        """
      Then query schema
        """
        root
         |-- date_cast: date (nullable = true)
         |-- date_try: date (nullable = true)
         |-- timestamp_cast: timestamp (nullable = true)
         |-- timestamp_try: timestamp (nullable = true)
         |-- ntz_cast: timestamp_ntz (nullable = true)
         |-- ntz_try: timestamp_ntz (nullable = true)
        """
