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

    Scenario: nested string and LTZ casts use the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(element_at(CAST(array('1970-01-01 00:00:00') AS ARRAY<TIMESTAMP>), 1)) AS array_micros,
          unix_micros(element_at(CAST(map('x', '1970-01-01 00:00:00') AS MAP<STRING,TIMESTAMP>), 'x')) AS map_micros,
          unix_micros(CAST(named_struct('x', '1970-01-01 00:00:00') AS STRUCT<x:TIMESTAMP>).x) AS struct_micros,
          element_at(CAST(array(TIMESTAMP '1970-01-01 00:00:00Z') AS ARRAY<STRING>), 1) AS array_value,
          element_at(CAST(map('x', TIMESTAMP '1970-01-01 00:00:00Z') AS MAP<STRING,STRING>), 'x') AS map_value,
          CAST(named_struct('x', TIMESTAMP '1970-01-01 00:00:00Z') AS STRUCT<x:STRING>).x AS struct_value,
          CAST(array(TIMESTAMP '1970-01-01 00:00:00Z') AS STRING) AS array_display,
          CAST(map('x', TIMESTAMP '1970-01-01 00:00:00Z') AS STRING) AS map_display,
          CAST(named_struct('x', TIMESTAMP '1970-01-01 00:00:00Z') AS STRING) AS struct_display
        """
      Then query result
        | array_micros | map_micros | struct_micros | array_value         | map_value           | struct_value        | array_display         | map_display                      | struct_display         |
        | -3723000000  | -3723000000 | -3723000000   | 1970-01-01 01:02:03 | 1970-01-01 01:02:03 | 1970-01-01 01:02:03 | [1970-01-01 01:02:03] | {x -> 1970-01-01 01:02:03} | {1970-01-01 01:02:03} |

    Scenario Outline: current date expressions use the session-local date
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          current_date() = CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE) AS current_date_result,
          curdate() = CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE) AS curdate_result,
          CAST('today' AS DATE) = CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE) AS today,
          CAST('tomorrow' AS DATE) = date_add(CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE), 1) AS tomorrow,
          CAST('yesterday' AS DATE) = date_add(CAST(CAST(current_timestamp() AS TIMESTAMP_NTZ) AS DATE), -1) AS yesterday
        """
      Then query result
        | current_date_result | curdate_result | today | tomorrow | yesterday |
        | true                | true           | true  | true     | true      |

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

    Scenario: to_date converts LTZ inputs in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          to_date(TIMESTAMP_LTZ '1969-12-31 23:30:00Z') AS unformatted,
          to_date(
            TIMESTAMP_LTZ '1969-12-31 23:30:00Z',
            'yyyy-MM-dd'
          ) AS formatted
        """
      Then query result
        | unformatted | formatted  |
        | 1970-01-01  | 1970-01-01 |

    Scenario Outline: no-format timestamp conversion supports typed inputs: <case>
      Given config spark.sql.session.timeZone = +01:02:03
      And config spark.sql.timestampType = <timestamp_type>
      When query
        """
        SELECT
          CAST(<function>(TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS STRING) AS from_ltz,
          CAST(<function>(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS STRING) AS from_ntz,
          CAST(<function>(DATE '1970-01-01') AS STRING) AS from_date
        """
      Then query result
        | from_ltz            | from_ntz            | from_date           |
        | 1970-01-01 01:02:03 | 1970-01-01 00:00:00 | 1970-01-01 00:00:00 |

      Examples:
        | case                                  | function         | timestamp_type |
        | to_timestamp with the LTZ default     | to_timestamp     | TIMESTAMP_LTZ  |
        | to_timestamp with the NTZ default     | to_timestamp     | TIMESTAMP_NTZ  |
        | to_timestamp_ltz                      | to_timestamp_ltz | TIMESTAMP_LTZ  |
        | to_timestamp_ntz                      | to_timestamp_ntz | TIMESTAMP_LTZ  |
        | try_to_timestamp with the LTZ default | try_to_timestamp | TIMESTAMP_LTZ  |
        | try_to_timestamp with the NTZ default | try_to_timestamp | TIMESTAMP_NTZ  |

    Scenario Outline: LTZ-to-LTZ timestamp conversion preserves the instant: <function>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT unix_micros(
          <function>(timestamp_micros(1762075800000000))
        ) AS result
        """
      Then query result
        | result           |
        | 1762075800000000 |

      Examples:
        | function         |
        | to_timestamp     |
        | to_timestamp_ltz |
        | try_to_timestamp |

    Scenario: convert_timezone converts LTZ inputs to session-local NTZ first
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          CAST(convert_timezone('UTC', 'UTC', TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS STRING) AS explicit_source,
          CAST(convert_timezone('UTC', TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS STRING) AS implicit_source
        """
      Then query result
        | explicit_source     | implicit_source     |
        | 1970-01-01 01:02:03 | 1970-01-01 00:00:00 |

    Scenario Outline: no-format datetime functions accept special values: <case>
      Given config spark.sql.session.timeZone = +01:02:03
      And config spark.sql.timestampType = <timestamp_type>
      When query
        """
        SELECT
          <function>('epoch') = CAST('epoch' AS <target_type>) AS epoch,
          <function>('now') IS NOT NULL AS now,
          <function>('today') = CAST('today' AS <target_type>) AS today,
          <function>('tomorrow') = CAST('tomorrow' AS <target_type>) AS tomorrow,
          <function>('yesterday') = CAST('yesterday' AS <target_type>) AS yesterday
        """
      Then query result
        | epoch | now  | today | tomorrow | yesterday |
        | true  | true | true  | true     | true      |

      Examples:
        | case                                  | function         | target_type   | timestamp_type |
        | to_date                               | to_date          | DATE          | TIMESTAMP_LTZ  |
        | to_timestamp with the LTZ default     | to_timestamp     | TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
        | to_timestamp with the NTZ default     | to_timestamp     | TIMESTAMP_NTZ | TIMESTAMP_NTZ  |
        | to_timestamp_ltz                      | to_timestamp_ltz | TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
        | to_timestamp_ntz                      | to_timestamp_ntz | TIMESTAMP_NTZ | TIMESTAMP_LTZ  |
        | try_to_timestamp with the LTZ default | try_to_timestamp | TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
        | try_to_timestamp with the NTZ default | try_to_timestamp | TIMESTAMP_NTZ | TIMESTAMP_NTZ  |

    Scenario Outline: no-format timestamp functions resolve now near the current instant: <case>
      Given config spark.sql.session.timeZone = +01:02:03
      And config spark.sql.timestampType = <timestamp_type>
      When query
        """
        SELECT abs(
          unix_micros(CAST(<function>('now') AS TIMESTAMP_LTZ))
          - unix_micros(current_timestamp())
        ) <= 60000000 AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                                  | function         | timestamp_type |
        | to_timestamp with the LTZ default     | to_timestamp     | TIMESTAMP_LTZ  |
        | to_timestamp with the NTZ default     | to_timestamp     | TIMESTAMP_NTZ  |
        | to_timestamp_ltz                      | to_timestamp_ltz | TIMESTAMP_LTZ  |
        | to_timestamp_ntz                      | to_timestamp_ntz | TIMESTAMP_LTZ  |
        | try_to_timestamp with the LTZ default | try_to_timestamp | TIMESTAMP_LTZ  |
        | try_to_timestamp with the NTZ default | try_to_timestamp | TIMESTAMP_NTZ  |

    Scenario: timestampadd preserves the input timestamp type and session semantics
      Given config spark.sql.session.timeZone = +01:02:03
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT
          timestampadd(SECOND, 1, TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ntz_result,
          timestampadd(SECOND, 1, TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS ltz_result,
          timestampadd(SECOND, -1, DATE '1970-01-01') AS date_result
        """
      Then query result
        | ntz_result          | ltz_result          | date_result         |
        | 1970-01-01 00:00:01 | 1970-01-01 01:02:04 | 1969-12-31 23:59:59 |
      And query schema
        """
        root
         |-- ntz_result: timestamp_ntz (nullable = false)
         |-- ltz_result: timestamp (nullable = false)
         |-- date_result: timestamp (nullable = false)
        """

    Scenario: timestamp applies Spark input-unit and session-zone conversions
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(timestamp(1)) AS numeric_seconds,
          unix_micros(timestamp(DATE '1970-01-01')) AS date_value,
          unix_micros(timestamp(TIMESTAMP_NTZ '1970-01-01 00:00:00')) AS ntz_value,
          unix_micros(timestamp(TIMESTAMP_LTZ '1970-01-01 00:00:00Z')) AS ltz_value
        """
      Then query result
        | numeric_seconds | date_value  | ntz_value   | ltz_value |
        | 1000000         | -3723000000 | -3723000000 | 0         |

    Scenario: struct serializers preserve second-precision session offsets
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          to_csv(named_struct('ts', TIMESTAMP '1970-01-01 00:00:00')) AS csv_default,
          to_csv(
            named_struct('ts', TIMESTAMP '1970-01-01 00:00:00'),
            map('timestampFormat', 'yyyy/MM/dd HH:mm:ss XXXXX')
          ) AS csv_custom,
          to_json(named_struct('ts', TIMESTAMP '1970-01-01 00:00:00')) AS json_default,
          to_json(
            named_struct('ts', TIMESTAMP '1970-01-01 00:00:00'),
            map('timestampFormat', 'yyyy/MM/dd HH:mm:ss XXXXX')
          ) AS json_custom,
          replace(
            to_xml(named_struct('ts', TIMESTAMP '1970-01-01 00:00:00')),
            '\n',
            '~'
          ) AS xml_default,
          replace(
            to_xml(
              named_struct('ts', TIMESTAMP '1970-01-01 00:00:00'),
              map('timestampFormat', 'yyyy/MM/dd HH:mm:ss XXXXX')
            ),
            '\n',
            '~'
          ) AS xml_custom
        """
      Then query result
        | csv_default                           | csv_custom                         | json_default                                    | json_custom                                  | xml_default                                                    | xml_custom                                                  |
        | 1970-01-01T00:00:00.000+01:02:03     | 1970/01/01 00:00:00 +01:02:03     | {"ts":"1970-01-01T00:00:00.000+01:02:03"}     | {"ts":"1970/01/01 00:00:00 +01:02:03"}     | <ROW>~    <ts>1970-01-01T00:00:00.000+01:02:03</ts>~</ROW>     | <ROW>~    <ts>1970/01/01 00:00:00 +01:02:03</ts>~</ROW>     |

    Scenario: timestampadd uses session-zone calendar arithmetic across DST
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_micros(
          timestampadd(
            HOUR,
            1,
            TIMESTAMP_LTZ '2025-03-09 01:30:00 America/Los_Angeles'
          )
        ) AS result
        """
      Then query result
        | result           |
        | 1741516200000000 |

    Scenario: unix time-unit functions coerce NTZ in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS micros,
          unix_millis(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS millis,
          unix_seconds(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS seconds
        """
      Then query result
        | micros      | millis   | seconds |
        | -3723000000 | -3723000 | -3723   |

    Scenario: make_timestamp_ltz supports a second-precision session offset
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(make_timestamp_ltz(DATE '1970-01-01', TIME '00:00:00')) AS implicit_zone,
          unix_micros(make_timestamp_ltz(DATE '1970-01-01', TIME '00:00:00', 'UTC')) AS explicit_zone,
          unix_micros(try_make_timestamp_ltz(DATE '1970-01-01', TIME '00:00:00')) AS try_implicit_zone,
          unix_micros(try_make_timestamp_ltz(DATE '1970-01-01', TIME '00:00:00', 'UTC')) AS try_explicit_zone
        """
      Then query result
        | implicit_zone | explicit_zone | try_implicit_zone | try_explicit_zone |
        | -3723000000   | 0             | -3723000000       | 0                 |

    Scenario Outline: window coerces <type> input in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT unix_micros(window(value, '1 day').start) AS result
        FROM VALUES (<input>) AS t(value)
        """
      Then query result
        | result       |
        | -86400000000 |

      Examples:
        | type   | input             |
        | DATE   | DATE '1970-01-01' |
        | STRING | '1970-01-01'      |

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

    Scenario Outline: fallible string casts and lossless temporal casts report correct nullability
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          <expression> AS bad,
          CAST(TIMESTAMP_NTZ '1970-01-01 00:00:00' AS TIMESTAMP) AS lossless
        """
      Then query result
        | bad  | lossless            |
        | NULL | 1970-01-01 00:00:00 |
      And query schema
        """
        root
         |-- bad: timestamp (nullable = true)
         |-- lossless: timestamp (nullable = false)
        """

      Examples:
        | ansi  | expression                    |
        | true  | TRY_CAST('bad' AS TIMESTAMP)  |
        | false | CAST('bad' AS TIMESTAMP)      |

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

    Scenario: timestamp interval arithmetic follows Spark through DST gaps and overlaps
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_micros(TIMESTAMP '2019-03-09 12:00:00' + INTERVAL 1 DAY) AS spring_day,
          unix_micros(TIMESTAMP '2019-11-02 12:00:00' + INTERVAL 1 DAY) AS fall_day,
          unix_micros(TIMESTAMP '2019-02-10 02:30:00' + INTERVAL 1 MONTH) AS gap_month,
          unix_micros(TIMESTAMP '2019-10-03 01:30:00-07:00' + INTERVAL 1 MONTH) AS overlap_early,
          unix_micros(TIMESTAMP '2019-12-03 01:30:00-08:00' - INTERVAL 1 MONTH) AS overlap_late,
          unix_micros(TIMESTAMP '2019-11-03 01:30:00-08:00' + INTERVAL 0 MONTH) AS overlap_zero,
          unix_micros(try_add(TIMESTAMP '2019-02-10 02:30:00', INTERVAL 1 MONTH)) AS try_gap,
          unix_micros(try_subtract(TIMESTAMP '2019-12-03 01:30:00-08:00', INTERVAL 1 MONTH)) AS try_overlap
        """
      Then query result
        | spring_day      | fall_day         | gap_month        | overlap_early    | overlap_late     | overlap_zero     | try_gap          | try_overlap      |
        | 1552244400000000 | 1572811200000000 | 1552213800000000 | 1572769800000000 | 1572773400000000 | 1572773400000000 | 1552213800000000 | 1572773400000000 |

    Scenario: timestampadd distinguishes calendar days from elapsed hours
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          CAST(timestampadd(DAY, 1, TIMESTAMP '2019-03-09 12:00:00') AS STRING) AS spring_day,
          CAST(TIMESTAMP '2019-03-09 12:00:00' + INTERVAL 24 HOURS AS STRING) AS spring_generic_hours,
          CAST(timestampadd(HOUR, 24, TIMESTAMP '2019-03-09 12:00:00') AS STRING) AS spring_hours,
          CAST(timestampadd(DAY, 1, TIMESTAMP '2019-11-02 12:00:00') AS STRING) AS fall_day,
          CAST(TIMESTAMP '2019-11-02 12:00:00' + INTERVAL 24 HOURS AS STRING) AS fall_generic_hours,
          CAST(timestampadd(HOUR, 24, TIMESTAMP '2019-11-02 12:00:00') AS STRING) AS fall_hours
        """
      Then query result
        | spring_day          | spring_generic_hours | spring_hours        | fall_day            | fall_generic_hours  | fall_hours          |
        | 2019-03-10 12:00:00 | 2019-03-10 12:00:00  | 2019-03-10 13:00:00 | 2019-11-03 12:00:00 | 2019-11-03 12:00:00 | 2019-11-03 11:00:00 |

    Scenario: calendar interval components apply months before days
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          CAST(TIMESTAMP '2019-02-09 02:30:00' + make_interval(0, 1, 0, 1) AS STRING) AS result,
          unix_micros(TIMESTAMP '2019-02-09 02:30:00' + make_interval(0, 1, 0, 1)) AS micros
        """
      Then query result
        | result              | micros           |
        | 2019-03-10 03:30:00 | 1552213800000000 |

    Scenario: try_add returns NULL on timestamp overflow
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT try_add(timestamp_micros(9223372036854775807), INTERVAL 1 MICROSECOND) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: timestamp subtraction uses session-local time across DST
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          TIMESTAMP '2019-03-10 12:00:00' - TIMESTAMP '2019-03-09 12:00:00' = INTERVAL 1 DAY AS spring,
          TIMESTAMP '2019-11-03 12:00:00' - TIMESTAMP '2019-11-02 12:00:00' = INTERVAL 1 DAY AS fall,
          try_subtract(TIMESTAMP '2019-03-10 12:00:00', TIMESTAMP '2019-03-09 12:00:00') = INTERVAL 1 DAY AS try_spring,
          TIMESTAMP_NTZ '2019-03-10 12:00:00' - TIMESTAMP_NTZ '2019-03-09 12:00:00' = INTERVAL 1 DAY AS ntz
        """
      Then query result
        | spring | fall | try_spring | ntz  |
        | true   | true | true       | true |

    Scenario: date and day-time interval arithmetic uses the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_micros(DATE '2024-03-10' + INTERVAL 2 HOURS) AS gap,
          unix_micros(INTERVAL 2 HOURS + DATE '2024-11-03') AS overlap,
          unix_micros(DATE '2024-03-10' - INTERVAL 2 HOURS) AS subtract,
          unix_micros(try_add(DATE '2024-03-10', INTERVAL 2 HOURS)) AS try_gap,
          unix_micros(try_subtract(DATE '2024-11-03', INTERVAL 2 HOURS)) AS try_overlap
        """
      Then query result
        | gap              | overlap         | subtract         | try_gap          | try_overlap      |
        | 1710064800000000 | 1730624400000000 | 1710050400000000 | 1710064800000000 | 1730610000000000 |

    Scenario: date and day-time interval arithmetic supports a second-precision session offset
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          unix_micros(DATE '1970-01-01' + INTERVAL 1 HOUR) AS add,
          unix_micros(INTERVAL 1 HOUR + DATE '1970-01-01') AS reverse_add,
          unix_micros(DATE '1970-01-01' - INTERVAL 1 HOUR) AS subtract
        """
      Then query result
        | add        | reverse_add | subtract   |
        | -123000000 | -123000000  | -7323000000 |

    Scenario: listagg renders LTZ values and delimiters in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          (
            SELECT listagg(ts, TIMESTAMP '1970-01-01 00:00:01Z')
            FROM VALUES
              (1, TIMESTAMP '1970-01-01 00:00:00Z'),
              (2, TIMESTAMP '1970-01-01 00:00:02Z') AS t(id, ts)
          ) AS aggregate_result,
          (
            SELECT first(result)
            FROM (
              SELECT listagg(ts, TIMESTAMP '1970-01-01 00:00:01Z') OVER (
                ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              ) AS result
              FROM VALUES
                (1, TIMESTAMP '1970-01-01 00:00:00Z'),
                (2, TIMESTAMP '1970-01-01 00:00:02Z') AS t(id, ts)
            )
          ) AS window_result
        """
      Then query result
        | aggregate_result                                          | window_result                                             |
        | 1970-01-01 01:02:031970-01-01 01:02:041970-01-01 01:02:05 | 1970-01-01 01:02:031970-01-01 01:02:041970-01-01 01:02:05 |

    Scenario: ANSI set operations and VALUES localize inputs aligned to LTZ
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          (
            SELECT sort_array(collect_list(unix_micros(ts)))
            FROM (
              SELECT TIMESTAMP_NTZ '1970-01-01 00:00:00' AS ts
              UNION ALL
              SELECT TIMESTAMP_LTZ '1970-01-01 00:00:00Z'
            )
          ) AS union_ntz,
          (
            SELECT sort_array(collect_list(unix_micros(ts)))
            FROM (
              SELECT DATE '1970-01-01' AS ts
              UNION ALL
              SELECT TIMESTAMP_LTZ '1970-01-01 00:00:00Z'
            )
          ) AS union_date,
          (
            SELECT sort_array(collect_list(unix_micros(ts)))
            FROM (
              SELECT '1970-01-01 00:00:00' AS ts
              UNION ALL
              SELECT TIMESTAMP_LTZ '1970-01-01 00:00:00Z'
            )
          ) AS union_string,
          (
            SELECT sort_array(collect_list(unix_micros(ts)))
            FROM VALUES
              (TIMESTAMP_NTZ '1970-01-01 00:00:00'),
              (TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS t(ts)
          ) AS values_ntz,
          (
            SELECT unix_micros(ts)
            FROM (
              SELECT TIMESTAMP_NTZ '1970-01-01 00:00:00' AS ts
              INTERSECT
              SELECT TIMESTAMP_LTZ '1969-12-31 22:57:57Z'
            )
          ) AS intersect_ntz,
          (
            SELECT sort_array(collect_list(unix_micros(ts)))
            FROM (
              (SELECT TIMESTAMP_NTZ '1970-01-01 00:00:00' AS ts
               UNION ALL
               SELECT TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
              EXCEPT
              SELECT TIMESTAMP_LTZ '1969-12-31 22:57:57Z'
            )
          ) AS except_ntz,
          (
            SELECT sort_array(collect_list(unix_micros(element_at(ts, 1))))
            FROM (
              SELECT array(TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ts
              UNION ALL
              SELECT array(TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
            )
          ) AS union_array,
          (
            SELECT sort_array(collect_list(unix_micros(element_at(ts, 'x'))))
            FROM (
              SELECT map('x', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ts
              UNION ALL
              SELECT map('x', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
            )
          ) AS union_map,
          (
            SELECT sort_array(collect_list(unix_micros(ts.x)))
            FROM (
              SELECT named_struct('x', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ts
              UNION ALL
              SELECT named_struct('x', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
            )
          ) AS union_struct
        """
      Then query result
        | union_ntz        | union_date       | union_string     | values_ntz       | intersect_ntz | except_ntz | union_array      | union_map        | union_struct     |
        | [-3723000000, 0] | [-3723000000, 0] | [-3723000000, 0] | [-3723000000, 0] | -3723000000   | [0]        | [-3723000000, 0] | [-3723000000, 0] | [-3723000000, 0] |

    Scenario: legacy set operations render LTZ values before widening to string
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          (
            SELECT sort_array(collect_list(ts))
            FROM (
              SELECT 'x' AS ts
              UNION ALL
              SELECT TIMESTAMP_LTZ '1970-01-01 00:00:00Z'
            )
          ) AS union_string,
          (
            SELECT sort_array(collect_list(element_at(ts, 1)))
            FROM (
              SELECT array('x') AS ts
              UNION ALL
              SELECT array(TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
            )
          ) AS nested_string
        """
      Then query result
        | union_string                 | nested_string                |
        | [1970-01-01 01:02:03, x]     | [1970-01-01 01:02:03, x]     |

    Scenario: nested LTZ common types use Spark's case-insensitive resolver by default
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = true
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT
          (
            SELECT sort_array(collect_list(unix_micros(ts.A)))
            FROM (
              SELECT named_struct('A', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ts
              UNION ALL
              SELECT named_struct('a', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
            )
          ) AS union_result,
          (
            SELECT sort_array(collect_list(unix_micros(ts.A)))
            FROM VALUES
              (named_struct('A', TIMESTAMP_NTZ '1970-01-01 00:00:00')),
              (named_struct('a', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')) AS t(ts)
          ) AS values_result
        """
      Then query result
        | union_result      | values_result     |
        | [-3723000000, 0] | [-3723000000, 0] |

    Scenario: case-sensitive LTZ set operations reject differently cased nested fields
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = true
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT named_struct('A', TIMESTAMP_NTZ '1970-01-01 00:00:00') AS ts
        UNION ALL
        SELECT named_struct('a', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')
        """
      Then query error (?i)(incompatible|different|struct|type|schema)

    Scenario: case-sensitive VALUES reject differently cased nested LTZ fields
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = true
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT *
        FROM VALUES
          (named_struct('A', TIMESTAMP_NTZ '1970-01-01 00:00:00')),
          (named_struct('a', TIMESTAMP_LTZ '1970-01-01 00:00:00Z')) AS t(ts)
        """
      Then query error (?i)(incompatible|different|struct|type|schema)

    Scenario: ANSI set operations reject string-to-timestamp map key coercion
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map('1970-01-01 00:00:00', 1) AS value
        UNION ALL
        SELECT map(TIMESTAMP_LTZ '1970-01-01 00:00:00', 2)
        """
      Then query error (?i)(incompatible|different|map|type)

    Scenario: legacy set operations widen temporal map keys to session-local strings
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT element_at(map_keys(value), 1) AS key
        FROM (
          SELECT map('1970-01-01 00:00:00', 1) AS value
          UNION ALL
          SELECT map(TIMESTAMP_LTZ '1970-01-01 00:00:00Z', 2)
        )
        ORDER BY key
        """
      Then query result
        | key                 |
        | 1970-01-01 00:00:00 |
        | 1970-01-01 01:02:03 |

    Scenario Outline: VALUES rejects string-to-timestamp map key coercion
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT *
        FROM VALUES
          (map('1970-01-01 00:00:00', 1)),
          (map(TIMESTAMP_LTZ '1970-01-01 00:00:00Z', 2)) AS t(value)
        """
      Then query error (?i)(incompatible|inconsistent).*type

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: VALUES rejects mixed string and LTZ inputs
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT *
        FROM VALUES
          ('x'),
          (TIMESTAMP_LTZ '1970-01-01 00:00:00Z') AS t(ts)
        """
      Then query error (?i)(incompatible|inconsistent).*type

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: lag and lead localize LTZ defaults in the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      When query
        """
        SELECT
          id,
          unix_micros(<expression> OVER (ORDER BY id)) AS micros
        FROM VALUES
          (1, TIMESTAMP_LTZ '1970-01-01 00:00:00Z'),
          (2, TIMESTAMP_LTZ '1970-01-01 00:00:01Z') AS t(id, ts)
        ORDER BY id
        """
      Then query result
        | id | micros      |
        | 1  | <first>     |
        | 2  | <second>    |

      Examples:
        | expression                                              | first       | second      |
        | lag(ts, 1, TIMESTAMP_NTZ '1970-01-01 00:00:00')         | -3723000000 | 0           |
        | lead(ts, 1, DATE '1970-01-01')                          | 1000000     | -3723000000 |
        | lead(ts, 1, '1970-01-01 00:00:00')                      | 1000000     | -3723000000 |

    Scenario: calendar consumers interpret LTZ instants in the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          hour(TIMESTAMP '2025-03-09 09:30:00Z') AS extracted_hour,
          date_part('HOUR', TIMESTAMP '2025-03-09 09:30:00Z') AS date_part_hour,
          to_date(TIMESTAMP '2025-03-09 07:30:00Z') AS local_date,
          last_day(TIMESTAMP '2025-03-01 07:30:00Z') AS local_last_day,
          next_day(TIMESTAMP '2025-03-10 00:30:00Z', 'MON') AS local_next_day,
          trunc(TIMESTAMP '2025-03-01 07:30:00Z', 'MONTH') AS local_trunc,
          months_between(
            TIMESTAMP '2000-02-29 00:00:00Z',
            TIMESTAMP '1997-03-01 00:00:00Z'
          ) AS local_months,
          timestampdiff(
            HOUR,
            TIMESTAMP '2019-03-09 12:00:00',
            TIMESTAMP '2019-03-10 12:00:00'
          ) AS local_hours
        """
      Then query result
        | extracted_hour | date_part_hour | local_date | local_last_day | local_next_day | local_trunc | local_months | local_hours |
        | 1              | 1              | 2025-03-08 | 2025-02-28     | 2025-03-10     | 2025-02-01  | 36.0         | 24          |

    Scenario: implicit temporal and string coercions use the session time zone
      Given config spark.sql.session.timeZone = +01:02:03
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          TIMESTAMP '1970-01-01 00:00:00Z' = '1970-01-01 01:02:03' AS equality,
          TIMESTAMP '1970-01-01 00:00:00Z' BETWEEN '1970-01-01 01:02:02' AND '1970-01-01 01:02:04' AS range_match,
          TIMESTAMP '1970-01-01 00:00:00Z' IN ('1970-01-01 01:02:03') AS in_match,
          DATE '2020-01-01' = 'bad' AS invalid_date_comparison,
          DATE '2020-01-01' IN ('bad') AS invalid_date_in,
          equal_null(TIMESTAMP '1970-01-01 00:00:00Z', '1970-01-01 01:02:03') AS null_safe_match,
          CASE WHEN true THEN TIMESTAMP '1970-01-01 00:00:00Z' ELSE 'x' END AS case_value,
          greatest(TIMESTAMP '1970-01-01 00:00:00Z', '0') AS greatest_value,
          array_join(array(TIMESTAMP '1970-01-01 00:00:00Z', 'x'), ',') AS array_value,
          concat_ws(',', TIMESTAMP '1970-01-01 00:00:00Z', array(TIMESTAMP '1970-01-01 00:00:00Z')) AS concatenated,
          array_contains(array(TIMESTAMP '1970-01-01 00:00:00'), DATE '1970-01-01') AS array_match,
          map_contains_key(map(TIMESTAMP '1970-01-01 00:00:00', 'x'), DATE '1970-01-01') AS map_match
        """
      Then query result
        | equality | range_match | in_match | invalid_date_comparison | invalid_date_in | null_safe_match | case_value          | greatest_value      | array_value            | concatenated                               | array_match | map_match |
        | true     | true        | true     | NULL                    | false           | true            | 1970-01-01 01:02:03 | 1970-01-01 01:02:03 | 1970-01-01 01:02:03,x | 1970-01-01 01:02:03,1970-01-01 01:02:03 | true        | true      |

    Scenario: row-dependent temporal comparisons parse strings across DST
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          id,
          value = local_text AS equality,
          value BETWEEN local_text AND local_text AS range_match,
          value IN (local_text) AS in_match
        FROM VALUES
          (1, TIMESTAMP '2025-03-09 09:30:00Z', '2025-03-09 01:30:00'),
          (2, TIMESTAMP '2025-03-09 10:30:00Z', '2025-03-09 03:30:00'),
          (3, TIMESTAMP '2025-11-02 08:30:00Z', '2025-11-02 01:30:00')
        AS t(id, value, local_text)
        ORDER BY id
        """
      Then query result
        | id | equality | range_match | in_match |
        | 1  | true     | true        | true     |
        | 2  | true     | true        | true     |
        | 3  | true     | true        | true     |
