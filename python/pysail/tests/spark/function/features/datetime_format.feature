Feature: datetime format strings

  Rule: Java datetime pattern formatting compatibility

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats ISO 8601 timestamp with literal separator
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-01 10:30:45', 'yyyy-MM-dd''T''HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2026-06-01T10:30:45 |

    Scenario: `date_format` formats standalone fractional seconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'SSSSSS') AS result
        """
      Then query result
        | result |
        | 123456 |

    Scenario: `date_format` formats text fields
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', 'EEEE, dd MMMM yyyy') AS result
        """
      Then query result
        | result               |
        | Monday, 15 June 2026 |

    Scenario: `date_format` formats a dynamic format expression
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', concat('yyyy-MM-dd', '''T''', 'HH:mm:ss')) AS result
        """
      Then query result
        | result              |
        | 2026-06-15T14:30:45 |

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
        | basic_date | local_date | local_time      | local_datetime            |
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
        | local_date  | offset_date | date_only  | datetime_with_zone          |
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
        | midnight   | noon       | last_second | with_nanos     |
        | 00:00:00   | 12:00:00   | 23:59:59    | 14:30:45.123456 |

    Scenario: `date_format` formats ISO_LOCAL_DATE_TIME with different components
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 00:00:00', 'ISO_LOCAL_DATE_TIME') AS midnight,
          date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'ISO_LOCAL_DATE_TIME') AS with_fractional,
          date_format(TIMESTAMP '2026-06-15 23:59:59', 'ISO_LOCAL_DATE_TIME') AS last_second
        """
      Then query result
        | midnight               | with_fractional               | last_second            |
        | 2026-06-15T00:00:00    | 2026-06-15T14:30:45.123456    | 2026-06-15T23:59:59    |

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
        | positive_offset  | negative_offset  | half_hour_offset | with_fractional     |
        | 12:30:45Z        | 19:30:45Z        | 09:00:45Z        | 12:30:45.123456Z    |

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
        | positive_offset           | negative_offset           | half_hour_offset         | with_fractional             |
        | 2026-06-15T12:30:45Z      | 2026-06-15T19:30:45Z      | 2026-06-15T09:00:45Z     | 2026-06-15T12:30:45.123456Z |

    Scenario: `date_format` formats ISO_INSTANT with different timezone inputs
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_INSTANT') AS utc_instant,
          date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'ISO_INSTANT') AS positive_offset,
          date_format(TIMESTAMP '2026-06-15 14:30:45-05:00', 'ISO_INSTANT') AS negative_offset
        """
      Then query result
        | utc_instant           | positive_offset       | negative_offset       |
        | 2026-06-15T14:30:45Z  | 2026-06-15T12:30:45Z  | 2026-06-15T19:30:45Z  |

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
        | min_date     | max_date     | min_timestamp              | max_timestamp              |
        | 0001-01-01   | 9999-12-31   | 0001-01-01T00:00:00Z[UTC]  | 9999-12-31T23:59:59Z[UTC]  |

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
        | zero_offset    | negative_zero  | max_positive    | max_negative    |
        | 2026-06-15Z    | 2026-06-15Z    | 2026-06-15Z     | 2026-06-16Z     |

    Scenario: `date_format` formats predefined ISO time variants
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'ISO_OFFSET_TIME') AS result
        """
      Then query result
        | result     |
        | 14:30:45Z  |

    Scenario: `date_format` formats predefined RFC 1123 date time
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45 UTC', 'RFC_1123_DATE_TIME') AS result
        """
      Then query result
        | result                       |
        | Mon, 15 Jun 2026 14:30:45 GMT |

  Rule: Edge cases and special scenarios

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` handles NULL timestamp
      When query
        """
        SELECT date_format(CAST(NULL AS TIMESTAMP), 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: `date_format` handles NULL format
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: `date_format` formats timestamp with nanoseconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                       |
        | 2026-06-15 14:30:45.123456000 |

    Scenario: `date_format` formats timestamp with timezone offset
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45+02:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 12:30:45Z        |

    Scenario: `date_format` formats date only
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2026-06-15 |

    Scenario: `date_format` formats with quarter
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'yyyy-Q-dd') AS result
        """
      Then query result
        | result      |
        | 2026-2-15   |

    Scenario: `date_format` formats with era
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'GGGG yyyy-MM-dd') AS result
        """
      Then query result
        | result                  |
        | Anno Domini 2026-06-15  |

    Scenario: `date_format` formats with week-based year
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'YYYY-ww-e') AS result
        """
      Then query result
        | result      |
        | 2026-25-1   |

    Scenario: `date_format` formats with day of year
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'yyyy-DDD') AS result
        """
      Then query result
        | result     |
        | 2026-166   |

  Rule: Width variation tests for month patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats month with width 1 (M)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'M') AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: `date_format` formats month with width 2 (MM)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'MM') AS result
        """
      Then query result
        | result |
        | 06     |

    Scenario: `date_format` formats month with width 3 (MMM)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'MMM') AS result
        """
      Then query result
        | result |
        | Jun    |

    Scenario: `date_format` formats month with width 4 (MMMM)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'MMMM') AS result
        """
      Then query result
        | result |
        | June   |

    Scenario: `date_format` formats month with width 5 (MMMMM)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'MMMMM') AS result
        """
      Then query result
        | result |
        | J      |

  Rule: Width variation tests for day-of-week patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats day-of-week with width 1 (E)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'E') AS result
        """
      Then query result
        | result |
        | Mon    |

    Scenario: `date_format` formats day-of-week with width 2 (EE)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'EE') AS result
        """
      Then query result
        | result |
        | Mon    |

    Scenario: `date_format` formats day-of-week with width 3 (EEE)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'EEE') AS result
        """
      Then query result
        | result |
        | Mon    |

    Scenario: `date_format` formats day-of-week with width 4 (EEEE)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'EEEE') AS result
        """
      Then query result
        | result  |
        | Monday  |

    Scenario: `date_format` formats day-of-week with width 5 (EEEEE)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'EEEEE') AS result
        """
      Then query result
        | result |
        | M      |

  Rule: Width variation tests for era patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats era with width 1 (G)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'G') AS result
        """
      Then query result
        | result |
        | AD     |

    Scenario: `date_format` formats era with width 2 (GG)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'GG') AS result
        """
      Then query result
        | result |
        | AD     |

    Scenario: `date_format` formats era with width 3 (GGG)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'GGG') AS result
        """
      Then query result
        | result |
        | AD     |

    Scenario: `date_format` formats era with width 5 (GGGGG)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'GGGGG') AS result
        """
      Then query result
        | result |
        | A      |

  Rule: Width variation tests for quarter patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats quarter with width 1 (Q)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'Q') AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: `date_format` formats quarter with width 2 (QQ)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'QQ') AS result
        """
      Then query result
        | result |
        | 02     |

    Scenario: `date_format` formats quarter with width 3 (QQQ)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'QQQ') AS result
        """
      Then query result
        | result |
        | Q2     |

    Scenario: `date_format` formats quarter with width 4 (QQQQ)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'QQQQ') AS result
        """
      Then query result
        | result        |
        | 2nd quarter   |

    Scenario: `date_format` formats quarter with width 5 (QQQQQ)
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'QQQQQ') AS result
        """
      Then query result
        | result |
        | 2      |

  Rule: Padding tests for numeric fields

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats day with and without padding
      When query
        """
        SELECT
          date_format(DATE '2026-06-05', 'd') AS no_pad,
          date_format(DATE '2026-06-05', 'dd') AS padded
        """
      Then query result
        | no_pad | padded |
        | 5      | 05     |

    Scenario: `date_format` formats hour with and without padding
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 05:30:45', 'H') AS no_pad,
          date_format(TIMESTAMP '2026-06-15 05:30:45', 'HH') AS padded
        """
      Then query result
        | no_pad | padded |
        | 5      | 05     |

    Scenario: `date_format` formats minute with and without padding
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:05:45', 'm') AS no_pad,
          date_format(TIMESTAMP '2026-06-15 14:05:45', 'mm') AS padded
        """
      Then query result
        | no_pad | padded |
        | 5      | 05     |

    Scenario: `date_format` formats second with and without padding
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:05', 's') AS no_pad,
          date_format(TIMESTAMP '2026-06-15 14:30:05', 'ss') AS padded
        """
      Then query result
        | no_pad | padded |
        | 5      | 05     |

  Rule: Fractional seconds width variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats fractional seconds with 1 digit (S)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'S') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: `date_format` formats fractional seconds with 2 digits (SS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SS') AS result
        """
      Then query result
        | result |
        | 12     |

    Scenario: `date_format` formats fractional seconds with 3 digits (SSS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SSS') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: `date_format` formats fractional seconds with 4 digits (SSSS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SSSS') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: `date_format` formats fractional seconds with 5 digits (SSSSS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SSSSS') AS result
        """
      Then query result
        | result |
        | 12345  |

    Scenario: `date_format` formats fractional seconds with 7 digits (SSSSSSS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SSSSSSS') AS result
        """
      Then query result
        | result  |
        | 1234560 |

    Scenario: `date_format` formats fractional seconds with 8 digits (SSSSSSSS)
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456789', 'SSSSSSSS') AS result
        """
      Then query result
        | result   |
        | 12345600 |

    Scenario: `date_format` handles leap year
      When query
        """
        SELECT date_format(DATE '2024-02-29', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: `date_format` handles year boundary
      When query
        """
        SELECT date_format(TIMESTAMP '2026-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2026-12-31 23:59:59 |

    Scenario: `date_format` handles epoch
      When query
        """
        SELECT date_format(TIMESTAMP '1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

  Rule: Extreme date and time values

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats minimum date (year 0001)
      When query
        """
        SELECT date_format(DATE '0001-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 0001-01-01 |

    Scenario: `date_format` formats maximum date (year 9999)
      When query
        """
        SELECT date_format(DATE '9999-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 9999-12-31 |

    Scenario: `date_format` formats minimum timestamp (year 0001)
      When query
        """
        SELECT date_format(TIMESTAMP '0001-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 0001-01-01 00:00:00 |

    Scenario: `date_format` formats maximum timestamp (year 9999)
      When query
        """
        SELECT date_format(TIMESTAMP '9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

    Scenario: `date_format` formats timestamp with maximum nanoseconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                       |
        | 2026-06-15 14:30:45.999999000 |

    Scenario: `date_format` formats timestamp with minimum nanoseconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                       |
        | 2026-06-15 14:30:45.000000000 |

    Scenario: `date_format` formats year 2038 boundary (32-bit overflow)
      When query
        """
        SELECT
          date_format(TIMESTAMP '2038-01-19 03:14:07', 'yyyy-MM-dd HH:mm:ss') AS before_overflow,
          date_format(TIMESTAMP '2038-01-19 03:14:08', 'yyyy-MM-dd HH:mm:ss') AS at_overflow
        """
      Then query result
        | before_overflow      | at_overflow           |
        | 2038-01-19 03:14:07 | 2038-01-19 03:14:08 |

    Scenario: `date_format` formats negative Unix epoch (before 1970)
      When query
        """
        SELECT
          date_format(TIMESTAMP '1969-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS before_epoch,
          date_format(TIMESTAMP '1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS year_1900
        """
      Then query result
        | before_epoch          | year_1900           |
        | 1969-12-31 23:59:59 | 1900-01-01 00:00:00 |

    Scenario: `date_format` formats leap year century (2000)
      When query
        """
        SELECT date_format(DATE '2000-02-29', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2000-02-29 |

    Scenario: `date_format` formats non-leap century (1900)
      When query
        """
        SELECT date_format(DATE '1900-02-28', 'yyyy-MM-dd') AS feb_28,
               date_format(DATE '1900-03-01', 'yyyy-MM-dd') AS mar_01
        """
      Then query result
        | feb_28    | mar_01    |
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
        | jan       | feb       | mar       | apr       | may       | jun       | jul       | aug       | sep       | oct       | nov       | dec       |
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
        | jan       | feb       | mar       | apr       | may       | jun       | jul       | aug       | sep       | oct       | nov       | dec       |
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
        | last_second          | last_nano                      |
        | 2026-06-15 23:59:59 | 2026-06-15 23:59:59.999999000 |

  Rule: Extreme date formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats minimum date (year 0001)
      When query
        """
        SELECT date_format(DATE '0001-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result    |
        | 0001-01-01 |

    Scenario: `date_format` formats maximum date (year 9999)
      When query
        """
        SELECT date_format(TIMESTAMP '9999-12-31 23:59:59.999999', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

  Rule: Nanosecond precision formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats maximum nanoseconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.999999000 |

    Scenario: `date_format` formats minimum nanoseconds
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.000000000 |

  Rule: Leap year formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats leap year 2024
      When query
        """
        SELECT date_format(DATE '2024-02-29', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result    |
        | 2024-02-29 |

    Scenario: `date_format` formats century leap year 2000
      When query
        """
        SELECT date_format(DATE '2000-02-29', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result    |
        | 2000-02-29 |

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

    Scenario: `to_timestamp` rejects extra argument
      When query
        """
        SELECT to_timestamp('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error (?i).*to_timestamp.*requires 1 or 2 arguments.*

    Scenario: `to_timestamp_ltz` rejects extra argument
      When query
        """
        SELECT to_timestamp_ltz('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error (?i).*to_timestamp.*requires 1 or 2 arguments.*

    Scenario: `to_timestamp_ntz` rejects extra argument
      When query
        """
        SELECT to_timestamp_ntz('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error (?i).*to_timestamp.*requires 1 or 2 arguments.*

    Scenario: `try_to_timestamp` rejects extra argument
      When query
        """
        SELECT try_to_timestamp('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error (?i).*try_to_timestamp.*requires 1 or 2 arguments.*
