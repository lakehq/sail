Feature: date_part / extract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: date_part second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT date_part('second', TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    Scenario: extract second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT extract(SECOND FROM TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    Scenario: date_part second of a nullable timestamp stays nullable
      When query
        """
        SELECT date_part('second', c) AS result FROM VALUES (TIMESTAMP '2024-01-01 00:00:05'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = true)
        """

    Scenario: interval components retain Spark's individual result types
      When query
        """
        SELECT date_part('year', INTERVAL '2-3' YEAR TO MONTH) AS years,
               date_part('month', INTERVAL '2-3' YEAR TO MONTH) AS months,
               date_part('day', INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS days,
               date_part('hour', INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS hours,
               date_part('minute', INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS minutes,
               date_part('second', INTERVAL '2 03:04:05.123456' DAY TO SECOND) AS seconds
        """
      Then query schema
        """
        root
         |-- years: integer (nullable = false)
         |-- months: byte (nullable = false)
         |-- days: integer (nullable = false)
         |-- hours: byte (nullable = false)
         |-- minutes: byte (nullable = false)
         |-- seconds: decimal(8,6) (nullable = false)
        """

    Scenario: parsed strings and timestamp without timezone date fields retain nullable casts
      When query
        """
        SELECT date_part('year', '2024') AS string_year,
               date_part('hour', '2024-01-15 12:30:00') AS string_hour,
               date_part('day', TIMESTAMP_NTZ '2024-01-15 12:30:00') AS ntz_day
        """
      Then query schema
        """
        root
         |-- string_year: integer (nullable = true)
         |-- string_hour: integer (nullable = true)
         |-- ntz_day: integer (nullable = true)
        """

    Scenario: time extraction retains nullable time function schemas
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT date_part('hour', TIME '12:34:56.123456') AS hours,
               date_part('minute', TIME '12:34:56.123456') AS minutes,
               date_part('second', TIME '12:34:56.123456') AS seconds
        """
      Then query schema
        """
        root
         |-- hours: integer (nullable = true)
         |-- minutes: integer (nullable = true)
         |-- seconds: decimal(8,6) (nullable = true)
        """

    Scenario: a null field produces a nullable double
      When query
        """
        SELECT date_part(CAST(NULL AS STRING), DATE '2024-01-07') AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Field names and date coercion

    Scenario: datepart and EXTRACT share date_part weekday semantics
      When query
        """
        SELECT datepart('DOW', DATE '2024-01-07') AS alias_weekday,
               date_part('DOW', DATE '2024-01-07') AS weekday,
               EXTRACT(DOW FROM DATE '2024-01-07') AS extracted_weekday
        """
      Then query result
        | alias_weekday | weekday | extracted_weekday |
        | 1             | 1       | 1                 |

    Scenario Outline: Spark field alias <field> extracts the expected component
      When query
        """
        SELECT date_part('<field>', <source>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | field          | source                          | result |
        | DOW            | DATE '2024-01-07'                | 1      |
        | DAYOFWEEK      | DATE '2024-01-07'                | 1      |
        | DAYOFWEEK_ISO  | DATE '2024-01-07'                | 7      |
        | DOW_ISO        | DATE '2024-01-08'                | 1      |
        | YEAROFWEEK     | DATE '2021-01-01'                | 2020   |
        | YRS            | DATE '2024-01-07'                | 2024   |
        | MONS           | DATE '2024-02-07'                | 2      |
        | W              | DATE '2021-01-01'                | 53     |
        | M              | TIMESTAMP '2024-01-07 12:34:56' | 34     |

    Scenario: date extraction parses partial and pre-nanosecond-range strings
      When query
        """
        SELECT id, date_part('year', c) AS result
        FROM VALUES (0, '2024'), (1, '1500-01-01'), (2, CAST(NULL AS STRING)) AS t(id, c)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 2024   |
        | 1  | 1500   |
        | 2  | NULL   |

    Scenario: timestamp strings use the session timezone per row
      Given config spark.sql.session.timeZone = Asia/Shanghai
      When query
        """
        SELECT id, date_part('hour', c) AS result
        FROM VALUES (0, '2024-01-15 12:30:00Z'), (1, '2024-01-15 13:30:00Z'),
          (2, CAST(NULL AS STRING)) AS t(id, c)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 20     |
        | 1  | 21     |
        | 2  | NULL   |

    Scenario: date extraction respects the local date of a timestamp
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT date_part('day', TIMESTAMP '2024-02-01 01:00:00Z') AS result
        """
      Then query result
        | result |
        | 31     |

    Scenario: stored timestamp expressions use the current session zone
      Given config spark.sql.session.timeZone = UTC
      And final statement
        """
        DROP VIEW IF EXISTS date_part_utc
        """
      And statement
        """
        CREATE TEMPORARY VIEW date_part_utc AS SELECT TIMESTAMP '2024-01-01 01:00:00' AS ts
        """
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT date_part('year', ts) AS years, date_part('day', ts) AS days,
               date_part('hour', ts) AS hours, dayofweek(ts) AS weekday
        FROM date_part_utc
        """
      Then query result
        | years | days | hours | weekday |
        | 2023  | 31   | 17    | 1       |

    Scenario: malformed strings return null with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT date_part('year', 'invalid') AS years, date_part('hour', 'invalid') AS hours
        """
      Then query result
        | years | hours |
        | NULL  | NULL  |

    Scenario: malformed strings raise with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT date_part('year', 'invalid') AS result
        """
      Then query error (?i)(invalid|malformed)

  Rule: Interval components

    Scenario: year-month interval columns retain signed remainders
      When query
        """
        SELECT id, date_part('year', c) AS years, date_part('month', c) AS months
        FROM VALUES (0, INTERVAL '2-3' YEAR TO MONTH),
          (1, INTERVAL '-2-3' YEAR TO MONTH),
          (2, CAST(NULL AS INTERVAL YEAR TO MONTH)) AS t(id, c)
        ORDER BY id
        """
      Then query result ordered
        | id | years | months |
        | 0  | 2     | 3      |
        | 1  | -2    | -3     |
        | 2  | NULL  | NULL   |

    Scenario: day-time interval columns extract signed components rather than totals
      When query
        """
        SELECT id, date_part('day', c) AS days, date_part('hour', c) AS hours,
               date_part('minute', c) AS minutes, date_part('second', c) AS seconds
        FROM VALUES
          (0, INTERVAL '2 03:04:05.123456' DAY TO SECOND),
          (1, INTERVAL '-2 03:04:05.123456' DAY TO SECOND),
          (2, CAST(NULL AS INTERVAL DAY TO SECOND)) AS t(id, c)
        ORDER BY id
        """
      Then query result ordered
        | id | days | hours | minutes | seconds   |
        | 0  | 2    | 3     | 4       | 5.123456  |
        | 1  | -2   | -3    | -4      | -5.123456 |
        | 2  | NULL | NULL  | NULL    | NULL      |

    Scenario: interval hour extraction does not overflow an intermediate total
      When query
        """
        SELECT date_part('hour', INTERVAL '106751991 04:00:54.775807' DAY TO SECOND) AS positive,
               date_part('hour', INTERVAL '-106751991 04:00:54.775807' DAY TO SECOND) AS negative
        """
      Then query result
        | positive | negative |
        | 4        | -4       |

    Scenario: calendar intervals carry whole days and retain signed fractional seconds
      When query
        """
        SELECT id, date_part('day', c) AS days, date_part('hour', c) AS hours,
               date_part('minute', c) AS minutes, date_part('second', c) AS seconds
        FROM VALUES
          (0, make_interval(0, 0, 0, 2, 27, 65, 65.123456)),
          (1, make_interval(0, 0, 0, -2, -27, -65, -65.123456)),
          (2, make_interval(0, 0, 0, 0, 0, 0, 1.123456)),
          (3, make_interval(CAST(NULL AS INT))),
          (4, make_interval(0, 0, 0, 2, -27)),
          (5, make_interval(0, 0, 0, 0, 0, 0, -0.000001)) AS t(id, c)
        ORDER BY id
        """
      Then query result ordered
        | id | days | hours | minutes | seconds   |
        | 0  | 3    | 4     | 6       | 5.123456  |
        | 1  | -3   | -4    | -6      | -5.123456 |
        | 2  | 0    | 0     | 0       | 1.123456  |
        | 3  | NULL | NULL  | NULL    | NULL      |
        | 4  | 1    | -3    | 0       | 0.000000  |
        | 5  | 0    | 0     | 0       | -0.000001 |

    Scenario Outline: carrying calendar days checks overflow with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT date_part('day', make_interval(0, 0, 0, 2147483647, 24)) AS result
        """
      Then query error (?i)overflow

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: timestamp fractions still include the second component before the epoch
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT date_part('second', timestamp_micros(-1)) AS result
        """
      Then query result
        | result    |
        | 59.999999 |

  Rule: Foldable and null fields

    # The field chooses the expression during analysis, so its value is required even
    # when other datetime functions can ignore unused scalar arguments at runtime.
    Scenario Outline: a failed constant field expression raises with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT date_part(element_at(array('YEAR'), 0), DATE '2024-01-07') AS result
        """
      Then query error (?i)index.*0

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: foldable string expressions select the field
      When query
        """
        SELECT date_part(upper('year'), DATE '2024-01-07') AS years,
               date_part(concat('D', 'OW'), DATE '2024-01-07') AS weekday
        """
      Then query result
        | years | weekday |
        | 2024  | 1       |

    Scenario: a null field discards source evaluation
      When query
        """
        SELECT date_part(concat(CAST(NULL AS STRING), 'year'), CAST(raise_error('source evaluated') AS TIMESTAMP)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: a null field discards the source aggregate before aggregation is resolved
      When query
        """
        SELECT count(*) AS rows FROM (
          SELECT date_part(CAST(NULL AS STRING), max(CAST(id AS TIMESTAMP))) AS result FROM range(0)
        )
        """
      Then query result
        | rows |
        | 0    |

    Scenario: a null field retains source cast analysis
      When query
        """
        SELECT date_part(CAST(NULL AS STRING), CAST(CAST(NULL AS ARRAY<INT>) AS TIMESTAMP)) AS result
        """
      Then query error (?i)(cannot|unsupported).*cast

    Scenario: a null field retains source function input analysis
      When query
        """
        SELECT date_part(CAST(NULL AS STRING), abs(array(1))) AS result
        """
      Then query error (?i)(numeric|coerc)

    Scenario Outline: a null field accepts <case> without evaluating the source
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT date_part(CAST(NULL AS STRING), <source>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                  | source             |
        | an array source       | array(1)           |
        | division by zero      | 1 / 0              |
        | invalid date parsing  | to_date('garbage') |

  @sail-only
  Rule: Existing native extensions

    Scenario: native aliases and additional fields remain available
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT date_part('isoyear', DATE '2021-01-01') AS iso_year,
               date_part('isodow', DATE '2024-01-07') AS iso_day,
               date_part('epoch', TIMESTAMP '1970-01-01 00:00:00.5') AS epoch,
               date_part('ms', TIMESTAMP '2024-01-07 00:00:05.123456') AS millis,
               date_part('us', TIMESTAMP '2024-01-07 00:00:05.123456') AS micros,
               date_part('nanoseconds', TIMESTAMP '2024-01-07 00:00:05.123456') AS nanos,
               date_part('"year"', DATE '2024-01-07') AS quoted_year,
               date_part('week', INTERVAL '14' DAY) AS weeks
        """
      Then query result
        | iso_year | iso_day | epoch | millis | micros  | nanos      | quoted_year | weeks |
        | 2020     | 7       | 0.5   | 5123   | 5123456 | 5123456000 | 2024        | 2     |

    Scenario: direct extraction helpers retain their interval extensions
      When query
        """
        SELECT day(INTERVAL '2' DAY) AS days, month(INTERVAL '14' MONTH) AS months
        """
      Then query result
        | days | months |
        | 2    | 2      |

  Rule: Fractional seconds retain valid timestamp values

    Scenario: Fractional seconds at timestamp boundaries preserve the non-null schema
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, date_part('second', timestamp_micros(IF(id=0,9223372036854775807L,-9223372036854775808L))) AS seconds
        FROM range(2) ORDER BY id
        """
      Then query result
        | id | seconds   |
        | 0  | 54.775807 |
        | 1  | 5.224192  |
      Then query schema
        """
        root
         |-- id: long (nullable = false)
         |-- seconds: decimal(8,6) (nullable = false)
        """

    Scenario: Fractional seconds preserve nullable and pre-epoch inputs
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT id, date_part('second',timestamp_micros(micros)) AS seconds
        FROM VALUES (0,9223372036854775807L),(1,-1L),(2,CAST(NULL AS BIGINT)) AS t(id,micros)
        ORDER BY id
        """
      Then query result
        | id | seconds   |
        | 0  | 54.775807 |
        | 1  | 59.999999 |
        | 2  | NULL      |

    Scenario: Fractional seconds retain historical timezone offset seconds
      Given config spark.sql.session.timeZone = Asia/Kathmandu
      When query
        """
        SELECT date_part('second',timestamp_micros(-2208988765000000L)) AS historical,
               date_part('second',timestamp_micros(-9223372036854775808L)) AS lower_seconds,
               date_part('second',timestamp_micros(9223372036854775807L)) AS upper_seconds
        """
      Then query result
        | historical | lower_seconds | upper_seconds |
        | 51.000000  | 21.224192     | 54.775807     |
