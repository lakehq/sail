# Moved from features/datetime_format.feature by the datetime/ layout reorganisation.
@datetime_format
Feature: datetime format strings

  Rule: Java datetime pattern formatting compatibility

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Java pattern: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '<ts>', <fmt>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                            | ts                         | fmt                                       | result               |
        | `date_format` formats ISO 8601 timestamp with literal separator | 2026-06-01 10:30:45        | 'yyyy-MM-dd''T''HH:mm:ss'                 | 2026-06-01T10:30:45  |
        | `date_format` formats standalone fractional seconds             | 2026-06-15 14:30:45.123456 | 'SSSSSS'                                  | 123456               |
        | `date_format` formats text fields                               | 2026-06-15 14:30:45        | 'EEEE, dd MMMM yyyy'                      | Monday, 15 June 2026 |
        | `date_format` formats a dynamic format expression               | 2026-06-15 14:30:45        | concat('yyyy-MM-dd', '''T''', 'HH:mm:ss') | 2026-06-15T14:30:45  |

  Rule: Java predefined DateTimeFormatter constants

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats predefined local ISO constants
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'BASIC_ISO_DATE') AS basic_date,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'ISO_LOCAL_DATE') AS local_date,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'ISO_LOCAL_TIME') AS local_time,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'ISO_LOCAL_DATE_TIME') AS local_datetime
        """
      Then query result
        | basic_date | local_date | local_time      | local_datetime             |
        | 20260615   | 2026-06-15 | 14:30:45.123456 | 2026-06-15T14:30:45.123456 |

    Scenario: `date_format` formats predefined ISO offset constants
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_OFFSET_DATE') AS offset_date,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_OFFSET_TIME') AS offset_time,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_OFFSET_DATE_TIME') AS offset_datetime,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_INSTANT') AS instant
        """
      Then query result
        | offset_date | offset_time | offset_datetime      | instant              |
        | 2026-06-15Z | 14:30:45Z   | 2026-06-15T14:30:45Z | 2026-06-15T14:30:45Z |

    Scenario: `date_format` formats predefined ISO date variants with optional offset
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'ISO_DATE') AS local_date,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_DATE') AS offset_date,
          date_format(DATE '2026-06-15', 'ISO_DATE') AS date_only,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_DATE_TIME') AS datetime_with_zone
        """
      Then query result
        | local_date  | offset_date | date_only  | datetime_with_zone        |
        | 2026-06-15Z | 2026-06-15Z | 2026-06-15 | 2026-06-15T14:30:45Z[UTC] |

    Scenario: `date_format` formats predefined ordinal and week dates
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'ISO_ORDINAL_DATE') AS ordinal_date,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'ISO_WEEK_DATE') AS week_date,
          date_format(DATE '2026-06-15', 'ISO_ORDINAL_DATE') AS ordinal_date_only,
          date_format(DATE '2026-06-15', 'ISO_WEEK_DATE') AS week_date_only
        """
      Then query result
        | ordinal_date | week_date   | ordinal_date_only | week_date_only |
        | 2026-166Z    | 2026-W25-1Z | 2026-166          | 2026-W25-1     |

    Scenario: `date_format` formats ISO_LOCAL_DATE with different input types
      When query
        """
        SELECT
          date_format(DATE '2026-06-15', 'ISO_LOCAL_DATE') AS date_only,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'ISO_LOCAL_DATE') AS timestamp_local,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_LOCAL_DATE') AS timestamp_utc
        """
      Then query result
        | date_only  | timestamp_local | timestamp_utc |
        | 2026-06-15 | 2026-06-15      | 2026-06-15    |

    Scenario: `date_format` formats ISO_LOCAL_TIME with different time components
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 00:00:00', 'ISO_LOCAL_TIME') AS midnight,
          date_format(TIMESTAMP '2026-06-15 12:00:00', 'ISO_LOCAL_TIME') AS noon,
          date_format(TIMESTAMP '2026-06-15 23:59:59', 'ISO_LOCAL_TIME') AS last_second,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'ISO_LOCAL_TIME') AS with_nanos
        """
      Then query result
        | midnight | noon     | last_second | with_nanos      |
        | 00:00:00 | 12:00:00 | 23:59:59    | 14:30:45.123456 |

    Scenario: `date_format` formats ISO_LOCAL_DATE_TIME with different components
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 00:00:00', 'ISO_LOCAL_DATE_TIME') AS midnight,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'ISO_LOCAL_DATE_TIME') AS with_fractional,
          date_format(TIMESTAMP '2026-06-15 23:59:59', 'ISO_LOCAL_DATE_TIME') AS last_second
        """
      Then query result
        | midnight            | with_fractional            | last_second         |
        | 2026-06-15T00:00:00 | 2026-06-15T14:30:45.123456 | 2026-06-15T23:59:59 |

    Scenario: `date_format` formats ISO_OFFSET_DATE with non-UTC timezones
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'ISO_OFFSET_DATE') AS positive_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-05:00', 'ISO_OFFSET_DATE') AS negative_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45+05:30', 'ISO_OFFSET_DATE') AS half_hour_offset
        """
      Then query result
        | positive_offset | negative_offset | half_hour_offset |
        | 2026-06-15Z     | 2026-06-15Z     | 2026-06-15Z      |

    Scenario: `date_format` formats ISO_OFFSET_TIME with non-UTC timezones
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'ISO_OFFSET_TIME') AS positive_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-05:00', 'ISO_OFFSET_TIME') AS negative_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45+05:30', 'ISO_OFFSET_TIME') AS half_hour_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456+02:00', 'ISO_OFFSET_TIME') AS with_fractional
        """
      Then query result
        | positive_offset | negative_offset | half_hour_offset | with_fractional  |
        | 12:30:45Z       | 19:30:45Z       | 09:00:45Z        | 12:30:45.123456Z |

    Scenario: `date_format` formats ISO_OFFSET_DATE_TIME with non-UTC timezones
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'ISO_OFFSET_DATE_TIME') AS positive_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-05:00', 'ISO_OFFSET_DATE_TIME') AS negative_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45+05:30', 'ISO_OFFSET_DATE_TIME') AS half_hour_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456+02:00', 'ISO_OFFSET_DATE_TIME') AS with_fractional
        """
      Then query result
        | positive_offset      | negative_offset      | half_hour_offset     | with_fractional             |
        | 2026-06-15T12:30:45Z | 2026-06-15T19:30:45Z | 2026-06-15T09:00:45Z | 2026-06-15T12:30:45.123456Z |

    Scenario: `date_format` formats ISO_INSTANT with different timezone inputs
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_INSTANT') AS utc_instant,
          date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'ISO_INSTANT') AS positive_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-05:00', 'ISO_INSTANT') AS negative_offset
        """
      Then query result
        | utc_instant          | positive_offset      | negative_offset      |
        | 2026-06-15T14:30:45Z | 2026-06-15T12:30:45Z | 2026-06-15T19:30:45Z |

    Scenario: `date_format` formats BASIC_ISO_DATE with different input types
      When query
        """
        SELECT
          date_format(DATE '2026-06-15', 'BASIC_ISO_DATE') AS date_only,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'BASIC_ISO_DATE') AS timestamp_local,
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'BASIC_ISO_DATE') AS timestamp_utc
        """
      Then query result
        | date_only | timestamp_local | timestamp_utc |
        | 20260615  | 20260615        | 20260615      |

    Scenario: `date_format` handles NULL with ISO formats
      When query
        """
        SELECT
          date_format(CAST(NULL AS TIMESTAMP), 'ISO_DATE') AS null_timestamp,
          date_format(CAST(NULL AS DATE), 'ISO_LOCAL_DATE') AS null_date,
          date_format(CAST(NULL AS TIMESTAMP), 'ISO_OFFSET_TIME') AS null_time
        """
      Then query result
        | null_timestamp | null_date | null_time |
        | NULL           | NULL      | NULL      |

    Scenario: `date_format` formats extreme dates with ISO formats
      When query
        """
        SELECT
          date_format(DATE '0001-01-01', 'ISO_DATE') AS min_date,
          date_format(DATE '9999-12-31', 'ISO_DATE') AS max_date,
          date_format(TIMESTAMP '0001-01-01 00:00:00', 'ISO_DATE_TIME') AS min_timestamp,
          date_format(TIMESTAMP '9999-12-31 23:59:59', 'ISO_DATE_TIME') AS max_timestamp
        """
      Then query result
        | min_date   | max_date   | min_timestamp             | max_timestamp             |
        | 0001-01-01 | 9999-12-31 | 0001-01-01T00:00:00Z[UTC] | 9999-12-31T23:59:59Z[UTC] |

    Scenario: `date_format` formats timezone offset edge cases
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45+00:00', 'ISO_OFFSET_DATE') AS zero_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-00:00', 'ISO_OFFSET_DATE') AS negative_zero,
          date_format(TIMESTAMP '2026-06-15 14:30:45+14:00', 'ISO_OFFSET_DATE') AS max_positive,
          date_format(TIMESTAMP '2026-06-15 14:30:45-12:00', 'ISO_OFFSET_DATE') AS max_negative
        """
      Then query result
        | zero_offset | negative_zero | max_positive | max_negative |
        | 2026-06-15Z | 2026-06-15Z   | 2026-06-15Z  | 2026-06-16Z  |

    Scenario Outline: Predefined constant: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | fmt                | result                        |
        | `date_format` formats predefined ISO time variants  | ISO_OFFSET_TIME    | 14:30:45Z                     |
        | `date_format` formats predefined RFC 1123 date time | RFC_1123_DATE_TIME | Mon, 15 Jun 2026 14:30:45 GMT |

  Rule: Edge cases and special scenarios

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Edge case: <case>
      When query
        """
        SELECT date_format(<input>, <fmt>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | input                                     | fmt                             | result                        |
        | `date_format` handles NULL timestamp                 | CAST(NULL AS TIMESTAMP)                   | 'yyyy-MM-dd'                    | NULL                          |
        | `date_format` handles NULL format                    | TIMESTAMP '2026-06-15 14:30:45'           | CAST(NULL AS STRING)            | NULL                          |
        | `date_format` formats timestamp with nanoseconds     | TIMESTAMP '2026-06-15 14:30:45.123456789' | 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS' | 2026-06-15 14:30:45.123456000 |
        | `date_format` formats timestamp with timezone offset | TIMESTAMP '2026-06-15 14:30:45+02:00'     | 'yyyy-MM-dd HH:mm:ssXXX'        | 2026-06-15 12:30:45Z          |
        | `date_format` formats date only                      | DATE '2026-06-15'                         | 'yyyy-MM-dd'                    | 2026-06-15                    |
        | `date_format` formats with quarter                   | DATE '2026-06-15'                         | 'yyyy-Q-dd'                     | 2026-2-15                     |
        | `date_format` formats with era                       | DATE '2026-06-15'                         | 'GGGG yyyy-MM-dd'               | Anno Domini 2026-06-15        |
        | `date_format` formats with week-based year           | DATE '2026-06-15'                         | 'YYYY-ww-e'                     | 2026-25-1                     |
        | `date_format` formats with day of year               | DATE '2026-06-15'                         | 'yyyy-DDD'                      | 2026-166                      |

  Rule: Width variation tests for month patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Month width: <case>
      When query
        """
        SELECT date_format(DATE '2026-06-15', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | fmt   | result |
        | `date_format` formats month with width 1 (M)     | M     | 6      |
        | `date_format` formats month with width 2 (MM)    | MM    | 06     |
        | `date_format` formats month with width 3 (MMM)   | MMM   | Jun    |
        | `date_format` formats month with width 4 (MMMM)  | MMMM  | June   |
        | `date_format` formats month with width 5 (MMMMM) | MMMMM | J      |

  Rule: Width variation tests for day-of-week patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Day-of-week width: <case>
      When query
        """
        SELECT date_format(DATE '2026-06-15', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | fmt   | result |
        | `date_format` formats day-of-week with width 1 (E)     | E     | Mon    |
        | `date_format` formats day-of-week with width 2 (EE)    | EE    | Mon    |
        | `date_format` formats day-of-week with width 3 (EEE)   | EEE   | Mon    |
        | `date_format` formats day-of-week with width 4 (EEEE)  | EEEE  | Monday |
        | `date_format` formats day-of-week with width 5 (EEEEE) | EEEEE | M      |

  Rule: Width variation tests for era patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Era width: <case>
      When query
        """
        SELECT date_format(DATE '2026-06-15', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | fmt   | result |
        | `date_format` formats era with width 1 (G)     | G     | AD     |
        | `date_format` formats era with width 2 (GG)    | GG    | AD     |
        | `date_format` formats era with width 3 (GGG)   | GGG   | AD     |
        | `date_format` formats era with width 5 (GGGGG) | GGGGG | A      |

  Rule: Width variation tests for quarter patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Quarter width: <case>
      When query
        """
        SELECT date_format(DATE '2026-06-15', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                               | fmt   | result      |
        | `date_format` formats quarter with width 1 (Q)     | Q     | 2           |
        | `date_format` formats quarter with width 2 (QQ)    | QQ    | 02          |
        | `date_format` formats quarter with width 3 (QQQ)   | QQQ   | Q2          |
        | `date_format` formats quarter with width 4 (QQQQ)  | QQQQ  | 2nd quarter |
        | `date_format` formats quarter with width 5 (QQQQQ) | QQQQQ | 2           |

  Rule: Padding tests for numeric fields

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Padding: <case>
      When query
        """
        SELECT
          date_format(<input>, '<no_pad_fmt>') AS no_pad,
          date_format(<input>, '<padded_fmt>') AS padded
        """
      Then query result
        | no_pad | padded |
        | 5      | 05     |

      Examples:
        | case                                                  | input                           | no_pad_fmt | padded_fmt |
        | `date_format` formats day with and without padding    | DATE '2026-06-05'               | d          | dd         |
        | `date_format` formats hour with and without padding   | TIMESTAMP '2026-06-15 05:30:45' | H          | HH         |
        | `date_format` formats minute with and without padding | TIMESTAMP '2026-06-15 14:05:45' | m          | mm         |
        | `date_format` formats second with and without padding | TIMESTAMP '2026-06-15 14:30:05' | s          | ss         |

  Rule: Fractional seconds width variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Fractional width: <case>
      When query
        """
        SELECT date_format(<input>, '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                              | input                                     | fmt                 | result              |
        | `date_format` formats fractional seconds with 1 digit (S)         | TIMESTAMP '2026-06-15 14:30:45.123456789' | S                   | 1                   |
        | `date_format` formats fractional seconds with 2 digits (SS)       | TIMESTAMP '2026-06-15 14:30:45.123456789' | SS                  | 12                  |
        | `date_format` formats fractional seconds with 3 digits (SSS)      | TIMESTAMP '2026-06-15 14:30:45.123456789' | SSS                 | 123                 |
        | `date_format` formats fractional seconds with 4 digits (SSSS)     | TIMESTAMP '2026-06-15 14:30:45.123456789' | SSSS                | 1234                |
        | `date_format` formats fractional seconds with 5 digits (SSSSS)    | TIMESTAMP '2026-06-15 14:30:45.123456789' | SSSSS               | 12345               |
        | `date_format` formats fractional seconds with 7 digits (SSSSSSS)  | TIMESTAMP '2026-06-15 14:30:45.123456789' | SSSSSSS             | 1234560             |
        | `date_format` formats fractional seconds with 8 digits (SSSSSSSS) | TIMESTAMP '2026-06-15 14:30:45.123456789' | SSSSSSSS            | 12345600            |
        | `date_format` handles leap year                                   | DATE '2024-02-29'                         | yyyy-MM-dd          | 2024-02-29          |
        | `date_format` handles year boundary                               | TIMESTAMP '2026-12-31 23:59:59'           | yyyy-MM-dd HH:mm:ss | 2026-12-31 23:59:59 |
        | `date_format` handles epoch                                       | TIMESTAMP '1970-01-01 00:00:00'           | yyyy-MM-dd HH:mm:ss | 1970-01-01 00:00:00 |

  Rule: Extreme date and time values

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Extreme value: <case>
      When query
        """
        SELECT date_format(<input>, '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | input                                     | fmt                           | result                        |
        | `date_format` formats minimum date (year 0001)           | DATE '0001-01-01'                         | yyyy-MM-dd                    | 0001-01-01                    |
        | `date_format` formats maximum date (year 9999)           | DATE '9999-12-31'                         | yyyy-MM-dd                    | 9999-12-31                    |
        | `date_format` formats minimum timestamp (year 0001)      | TIMESTAMP '0001-01-01 00:00:00'           | yyyy-MM-dd HH:mm:ss           | 0001-01-01 00:00:00           |
        | `date_format` formats maximum timestamp (year 9999)      | TIMESTAMP '9999-12-31 23:59:59'           | yyyy-MM-dd HH:mm:ss           | 9999-12-31 23:59:59           |
        | `date_format` formats timestamp with maximum nanoseconds | TIMESTAMP '2026-06-15 14:30:45.999999999' | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45.999999000 |
        | `date_format` formats timestamp with minimum nanoseconds | TIMESTAMP '2026-06-15 14:30:45.000000001' | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45.000000000 |
        | `date_format` formats leap year century (2000)           | DATE '2000-02-29'                         | yyyy-MM-dd                    | 2000-02-29                    |

    Scenario: `date_format` formats year 2038 boundary (32-bit overflow)
      When query
        """
        SELECT
          date_format(TIMESTAMP '2038-01-19 03:14:07', 'yyyy-MM-dd HH:mm:ss') AS before_overflow,
          date_format(TIMESTAMP '2038-01-19 03:14:08', 'yyyy-MM-dd HH:mm:ss') AS at_overflow
        """
      Then query result
        | before_overflow     | at_overflow         |
        | 2038-01-19 03:14:07 | 2038-01-19 03:14:08 |

    Scenario: `date_format` formats negative Unix epoch (before 1970)
      When query
        """
        SELECT
          date_format(TIMESTAMP '1969-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS before_epoch,
          date_format(TIMESTAMP '1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS year_1900
        """
      Then query result
        | before_epoch        | year_1900           |
        | 1969-12-31 23:59:59 | 1900-01-01 00:00:00 |

    Scenario: `date_format` formats non-leap century (1900)
      When query
        """
        SELECT date_format(DATE '1900-02-28', 'yyyy-MM-dd') AS feb_28,
               date_format(DATE '1900-03-01', 'yyyy-MM-dd') AS mar_01
        """
      Then query result
        | feb_28     | mar_01     |
        | 1900-02-28 | 1900-03-01 |

    Scenario: `date_format` formats first day of each month
      When query
        """
        SELECT
          date_format(DATE '2026-01-01', 'yyyy-MM-dd') AS jan,
          date_format(DATE '2026-02-01', 'yyyy-MM-dd') AS feb,
          date_format(DATE '2026-03-01', 'yyyy-MM-dd') AS mar,
          date_format(DATE '2026-04-01', 'yyyy-MM-dd') AS apr,
          date_format(DATE '2026-05-01', 'yyyy-MM-dd') AS may,
          date_format(DATE '2026-06-01', 'yyyy-MM-dd') AS jun,
          date_format(DATE '2026-07-01', 'yyyy-MM-dd') AS jul,
          date_format(DATE '2026-08-01', 'yyyy-MM-dd') AS aug,
          date_format(DATE '2026-09-01', 'yyyy-MM-dd') AS sep,
          date_format(DATE '2026-10-01', 'yyyy-MM-dd') AS oct,
          date_format(DATE '2026-11-01', 'yyyy-MM-dd') AS nov,
          date_format(DATE '2026-12-01', 'yyyy-MM-dd') AS dec
        """
      Then query result
        | jan        | feb        | mar        | apr        | may        | jun        | jul        | aug        | sep        | oct        | nov        | dec        |
        | 2026-01-01 | 2026-02-01 | 2026-03-01 | 2026-04-01 | 2026-05-01 | 2026-06-01 | 2026-07-01 | 2026-08-01 | 2026-09-01 | 2026-10-01 | 2026-11-01 | 2026-12-01 |

    Scenario: `date_format` formats last day of each month
      When query
        """
        SELECT
          date_format(DATE '2026-01-31', 'yyyy-MM-dd') AS jan,
          date_format(DATE '2026-02-28', 'yyyy-MM-dd') AS feb,
          date_format(DATE '2026-03-31', 'yyyy-MM-dd') AS mar,
          date_format(DATE '2026-04-30', 'yyyy-MM-dd') AS apr,
          date_format(DATE '2026-05-31', 'yyyy-MM-dd') AS may,
          date_format(DATE '2026-06-30', 'yyyy-MM-dd') AS jun,
          date_format(DATE '2026-07-31', 'yyyy-MM-dd') AS jul,
          date_format(DATE '2026-08-31', 'yyyy-MM-dd') AS aug,
          date_format(DATE '2026-09-30', 'yyyy-MM-dd') AS sep,
          date_format(DATE '2026-10-31', 'yyyy-MM-dd') AS oct,
          date_format(DATE '2026-11-30', 'yyyy-MM-dd') AS nov,
          date_format(DATE '2026-12-31', 'yyyy-MM-dd') AS dec
        """
      Then query result
        | jan        | feb        | mar        | apr        | may        | jun        | jul        | aug        | sep        | oct        | nov        | dec        |
        | 2026-01-31 | 2026-02-28 | 2026-03-31 | 2026-04-30 | 2026-05-31 | 2026-06-30 | 2026-07-31 | 2026-08-31 | 2026-09-30 | 2026-10-31 | 2026-11-30 | 2026-12-31 |

    Scenario: `date_format` formats time at midnight boundary
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS midnight,
          date_format(TIMESTAMP '2026-06-15 00:00:00.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS midnight_nano
        """
      Then query result
        | midnight            | midnight_nano                 |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00.000000000 |

    Scenario: `date_format` formats time at last second of day
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS last_second,
          date_format(TIMESTAMP '2026-06-15 23:59:59.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS last_nano
        """
      Then query result
        | last_second         | last_nano                     |
        | 2026-06-15 23:59:59 | 2026-06-15 23:59:59.999999000 |

  Rule: Extreme date formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Extreme date: <case>
      When query
        """
        SELECT date_format(<input>, '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | input                                  | fmt                 | result              |
        | `date_format` formats minimum date (year 0001) | DATE '0001-01-01'                      | yyyy-MM-dd          | 0001-01-01          |
        | `date_format` formats maximum date (year 9999) | TIMESTAMP '9999-12-31 23:59:59.999999' | yyyy-MM-dd HH:mm:ss | 9999-12-31 23:59:59 |

  Rule: Nanosecond precision formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Nanosecond precision: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '<ts>', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | ts                            | result                        |
        | `date_format` formats maximum nanoseconds | 2026-06-15 14:30:45.999999999 | 2026-06-15 14:30:45.999999000 |
        | `date_format` formats minimum nanoseconds | 2026-06-15 14:30:45.000000001 | 2026-06-15 14:30:45.000000000 |

  Rule: Leap year formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Leap year: <case>
      When query
        """
        SELECT date_format(DATE '<d>', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | d          | result     |
        | `date_format` formats leap year 2024         | 2024-02-29 | 2024-02-29 |
        | `date_format` formats century leap year 2000 | 2000-02-29 | 2000-02-29 |

  Rule: Year 2038 boundary formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats year 2038 boundary
      When query
        """
        SELECT date_format(TIMESTAMP '2038-01-19 03:14:07', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2038-01-19 03:14:07 |

  Rule: Spark-compatible datetime formatting arities

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` rejects extra locale argument
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'EEEE, dd MMMM yyyy a QQQQ GGGG', 'extra')
        """
      Then query error (?i).*date_format.*requires 2 arguments.*

    Scenario: `from_unixtime` rejects extra locale argument
      When query
        """
        SELECT from_unixtime(1781533845, 'EEEE, dd MMMM yyyy a QQQQ GGGG', 'extra')
        """
      Then query error (?i).*from_unixtime.*requires 1 or 2 arguments.*

    Scenario Outline: Extra argument: <case>
      When query
        """
        SELECT <fn>('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error <error>

      Examples:
        | case                                      | fn               | error                                               |
        | `to_timestamp` rejects extra argument     | to_timestamp     | (?i).*to_timestamp.*requires 1 or 2 arguments.*     |
        | `to_timestamp_ltz` rejects extra argument | to_timestamp_ltz | (?i).*to_timestamp.*requires 1 or 2 arguments.*     |
        | `to_timestamp_ntz` rejects extra argument | to_timestamp_ntz | (?i).*to_timestamp.*requires 1 or 2 arguments.*     |
        | `try_to_timestamp` rejects extra argument | try_to_timestamp | (?i).*try_to_timestamp.*requires 1 or 2 arguments.* |
