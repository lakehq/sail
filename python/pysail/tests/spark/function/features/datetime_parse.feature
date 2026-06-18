Feature: datetime parsing with format strings

  Rule: Java predefined DateTimeFormatter constants for parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses ISO_LOCAL_DATE_TIME format
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45', 'ISO_LOCAL_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

    Scenario: `to_timestamp` parses ISO_OFFSET_DATE_TIME format
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45Z', 'ISO_OFFSET_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

    Scenario: `to_timestamp` parses ISO_INSTANT format
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45Z', 'ISO_INSTANT') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

    Scenario: `to_timestamp` parses ISO_LOCAL_DATE format
      When query
        """
        SELECT to_timestamp('2026-06-15', 'ISO_LOCAL_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_LOCAL_TIME format
      When query
        """
        SELECT to_timestamp('14:30:45', 'ISO_LOCAL_TIME') AS result
        """
      Then query result
        | result                      |
        | 1970-01-01 14:30:45        |

    Scenario: `to_timestamp` parses BASIC_ISO_DATE format
      When query
        """
        SELECT to_timestamp('20260615', 'BASIC_ISO_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_ORDINAL_DATE format
      When query
        """
        SELECT to_timestamp('2026-166', 'ISO_ORDINAL_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_WEEK_DATE format
      When query
        """
        SELECT to_timestamp('2026-W25-1', 'ISO_WEEK_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_DATE format with offset
      When query
        """
        SELECT to_timestamp('2026-06-15Z', 'ISO_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_DATE format without offset
      When query
        """
        SELECT to_timestamp('2026-06-15', 'ISO_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_TIME format with offset
      When query
        """
        SELECT to_timestamp('14:30:45Z', 'ISO_TIME') AS result
        """
      Then query result
        | result                      |
        | 1970-01-01 14:30:45        |

    Scenario: `to_timestamp` parses ISO_TIME format without offset
      When query
        """
        SELECT to_timestamp('14:30:45.123', 'ISO_TIME') AS result
        """
      Then query result
        | result                      |
        | 1970-01-01 14:30:45.123    |

    Scenario: `to_timestamp` parses ISO_OFFSET_DATE format
      When query
        """
        SELECT to_timestamp('2026-06-15Z', 'ISO_OFFSET_DATE') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses ISO_OFFSET_TIME format
      When query
        """
        SELECT to_timestamp('14:30:45Z', 'ISO_OFFSET_TIME') AS result
        """
      Then query result
        | result                      |
        | 1970-01-01 14:30:45        |

    Scenario: `to_timestamp` parses ISO_DATE_TIME format with zone
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45Z[UTC]', 'ISO_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

    Scenario: `to_timestamp` parses ISO_DATE_TIME format without offset
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45.123456', 'ISO_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: `to_timestamp` parses ISO_8601 format alias
      When query
        """
        SELECT to_timestamp('2026-06-01T10:30:45', 'ISO_8601') AS result
        """
      Then query result
        | result              |
        | 2026-06-01 10:30:45 |

    Scenario: `to_timestamp` parses ISO_ZONED_DATE_TIME format
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45Z[UTC]', 'ISO_ZONED_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

    Scenario: `to_timestamp` parses RFC_1123_DATE_TIME format
      When query
        """
        SELECT to_timestamp('Mon, 15 Jun 2026 14:30:45 GMT', 'RFC_1123_DATE_TIME') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

  Rule: to_date with predefined formatters

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_date` parses ISO_LOCAL_DATE format
      When query
        """
        SELECT to_date('2026-06-15', 'ISO_LOCAL_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses BASIC_ISO_DATE format
      When query
        """
        SELECT to_date('20260615', 'BASIC_ISO_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses ISO_ORDINAL_DATE format
      When query
        """
        SELECT to_date('2026-166', 'ISO_ORDINAL_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses ISO_WEEK_DATE format
      When query
        """
        SELECT to_date('2026-W25-1', 'ISO_WEEK_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses ISO_DATE format with offset
      When query
        """
        SELECT to_date('2026-06-15Z', 'ISO_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses ISO_DATE format without offset
      When query
        """
        SELECT to_date('2026-06-15', 'ISO_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_date` parses ISO_OFFSET_DATE format
      When query
        """
        SELECT to_date('2026-06-15Z', 'ISO_OFFSET_DATE') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

  Rule: Error handling for invalid formats

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` errors on invalid ISO_LOCAL_DATE_TIME
      When query
        """
        SELECT to_timestamp('invalid', 'ISO_LOCAL_DATE_TIME')
        """
      Then query error

    Scenario: `to_date` errors on invalid BASIC_ISO_DATE
      When query
        """
        SELECT to_date('invalid', 'BASIC_ISO_DATE')
        """
      Then query error

    Scenario: `to_timestamp` errors on mismatched format
      When query
        """
        SELECT to_timestamp('2026-06-15', 'ISO_LOCAL_TIME')
        """
      Then query error

  Rule: Parsing with custom format patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses custom format with literal separator
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd'T'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses custom format with fractional seconds
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: `to_date` parses custom format
      When query
        """
        SELECT to_date('2026/06/15', 'yyyy/MM/dd') AS result
        """
      Then query result
        | result    |
        | 2026-06-15 |

    Scenario: `to_timestamp` parses with month name
      When query
        """
        SELECT to_timestamp('15 June 2026', 'dd MMMM yyyy') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses with day name
      When query
        """
        SELECT to_timestamp('Monday, 15 June 2026', 'EEEE, dd MMMM yyyy') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp_ltz` parses custom format with offset
      When query
        """
        SELECT to_timestamp_ltz('2026-06-15T16:30:45+02:00', "yyyy-MM-dd'T'HH:mm:ssXXX") AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp_ntz` parses custom format with offset
      When query
        """
        SELECT to_timestamp_ntz('2026-06-15T16:30:45+02:00', "yyyy-MM-dd'T'HH:mm:ssXXX") AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

  Rule: Day-of-week parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses day-of-week with width 1-3 (E, EE, EEE)
      When query
        """
        SELECT
          to_timestamp('Mon, 15 June 2026', 'EEE, dd MMMM yyyy') AS day_mon,
          to_timestamp('Tue, 16 June 2026', 'EEE, dd MMMM yyyy') AS day_tue,
          to_timestamp('Sun, 14 June 2026', 'EEE, dd MMMM yyyy') AS day_sun
        """
      Then query result
        | day_mon             | day_tue             | day_sun             |
        | 2026-06-15 00:00:00 | 2026-06-16 00:00:00 | 2026-06-14 00:00:00 |

    Scenario: `to_timestamp` parses day-of-week with width 4 (EEEE)
      When query
        """
        SELECT
          to_timestamp('Monday, 15 June 2026', 'EEEE, dd MMMM yyyy') AS day_monday,
          to_timestamp('Sunday, 14 June 2026', 'EEEE, dd MMMM yyyy') AS day_sunday
        """
      Then query result
        | day_monday          | day_sunday          |
        | 2026-06-15 00:00:00 | 2026-06-14 00:00:00 |

  Rule: Quarter parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses quarter with width 1 (Q)
      When query
        """
        SELECT
          to_timestamp('2026 Q2', 'yyyy Q') AS q2,
          to_timestamp('2026 Q4', 'yyyy Q') AS q4
        """
      Then query result
        | q2                  | q4                  |
        | 2026-04-01 00:00:00 | 2026-10-01 00:00:00 |

    Scenario: `to_timestamp` parses quarter with width 2 (QQ)
      When query
        """
        SELECT
          to_timestamp('2026 Q02', 'yyyy QQ') AS q2,
          to_timestamp('2026 Q04', 'yyyy QQ') AS q4
        """
      Then query result
        | q2                  | q4                  |
        | 2026-04-01 00:00:00 | 2026-10-01 00:00:00 |

    Scenario: `to_timestamp` parses quarter with width 3 (QQQ)
      When query
        """
        SELECT
          to_timestamp('2026 Q2', 'yyyy QQQ') AS q2,
          to_timestamp('2026 Q4', 'yyyy QQQ') AS q4
        """
      Then query result
        | q2                  | q4                  |
        | 2026-04-01 00:00:00 | 2026-10-01 00:00:00 |

    Scenario: `to_timestamp` parses quarter with width 4 (QQQQ)
      When query
        """
        SELECT
          to_timestamp('2026 2nd quarter', 'yyyy QQQQ') AS q2,
          to_timestamp('2026 4th quarter', 'yyyy QQQQ') AS q4
        """
      Then query result
        | q2                  | q4                  |
        | 2026-04-01 00:00:00 | 2026-10-01 00:00:00 |

    Scenario: `to_timestamp` parses 12-hour timestamp with AM/PM marker
      When query
        """
        SELECT to_timestamp('5/9/2026 4:53:33 PM', 'M/d/yyyy h:mm:ss a') AS result
        """
      Then query result
        | result              |
        | 2026-05-09 16:53:33 |

  Rule: AM/PM marker parsing variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses AM marker
      When query
        """
        SELECT
          to_timestamp('2026-06-15 01:30:45 AM', 'yyyy-MM-dd hh:mm:ss a') AS hour_1_am,
          to_timestamp('2026-06-15 12:00:00 AM', 'yyyy-MM-dd hh:mm:ss a') AS midnight_am
        """
      Then query result
        | hour_1_am           | midnight_am         |
        | 2026-06-15 01:30:45 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses PM marker
      When query
        """
        SELECT
          to_timestamp('2026-06-15 01:30:45 PM', 'yyyy-MM-dd hh:mm:ss a') AS hour_1_pm,
          to_timestamp('2026-06-15 12:00:00 PM', 'yyyy-MM-dd hh:mm:ss a') AS noon_pm
        """
      Then query result
        | hour_1_pm           | noon_pm             |
        | 2026-06-15 13:30:45 | 2026-06-15 12:00:00 |

    Scenario: `to_timestamp` parses lowercase am/pm
      When query
        """
        SELECT
          to_timestamp('2026-06-15 01:30:45 am', 'yyyy-MM-dd hh:mm:ss a') AS hour_1_am,
          to_timestamp('2026-06-15 01:30:45 pm', 'yyyy-MM-dd hh:mm:ss a') AS hour_1_pm
        """
      Then query result
        | hour_1_am           | hour_1_pm           |
        | 2026-06-15 01:30:45 | 2026-06-15 13:30:45 |

  Rule: Era parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses AD era
      When query
        """
        SELECT
          to_timestamp('AD 2026-06-15', 'G yyyy-MM-dd') AS era_ad,
          to_timestamp('2026-06-15 AD', 'yyyy-MM-dd G') AS era_ad_suffix
        """
      Then query result
        | era_ad              | era_ad_suffix       |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses CE era
      When query
        """
        SELECT
          to_timestamp('CE 2026-06-15', 'G yyyy-MM-dd') AS era_ce,
          to_timestamp('2026-06-15 CE', 'yyyy-MM-dd G') AS era_ce_suffix
        """
      Then query result
        | era_ce              | era_ce_suffix       |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` handles NULL input
      When query
        """
        SELECT to_timestamp(CAST(NULL AS STRING), 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: `to_date` handles NULL input
      When query
        """
        SELECT to_date(CAST(NULL AS STRING), 'yyyy-MM-dd') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: `to_timestamp` handles NULL format
      When query
        """
        SELECT to_timestamp('2026-06-15', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Padding variations for numeric fields

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses day with and without padding
      When query
        """
        SELECT
          to_timestamp('2026-06-5', 'yyyy-MM-d') AS day_5,
          to_timestamp('2026-06-05', 'yyyy-MM-dd') AS day_05,
          to_timestamp('2026-06-15', 'yyyy-MM-dd') AS day_15
        """
      Then query result
        | day_5               | day_05              | day_15              |
        | 2026-06-05 00:00:00 | 2026-06-05 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses hour with and without padding
      When query
        """
        SELECT
          to_timestamp('2026-06-15 5:30:45', 'yyyy-MM-dd H:mm:ss') AS hour_5,
          to_timestamp('2026-06-15 05:30:45', 'yyyy-MM-dd HH:mm:ss') AS hour_05,
          to_timestamp('2026-06-15 15:30:45', 'yyyy-MM-dd HH:mm:ss') AS hour_15
        """
      Then query result
        | hour_5              | hour_05             | hour_15             |
        | 2026-06-15 05:30:45 | 2026-06-15 05:30:45 | 2026-06-15 15:30:45 |

    Scenario: `to_timestamp` parses minute with and without padding
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:5:45', 'yyyy-MM-dd HH:m:ss') AS min_5,
          to_timestamp('2026-06-15 14:05:45', 'yyyy-MM-dd HH:mm:ss') AS min_05,
          to_timestamp('2026-06-15 14:55:45', 'yyyy-MM-dd HH:mm:ss') AS min_55
        """
      Then query result
        | min_5               | min_05              | min_55              |
        | 2026-06-15 14:05:45 | 2026-06-15 14:05:45 | 2026-06-15 14:55:45 |

    Scenario: `to_timestamp` parses second with and without padding
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:5', 'yyyy-MM-dd HH:mm:s') AS sec_5,
          to_timestamp('2026-06-15 14:30:05', 'yyyy-MM-dd HH:mm:ss') AS sec_05,
          to_timestamp('2026-06-15 14:30:55', 'yyyy-MM-dd HH:mm:ss') AS sec_55
        """
      Then query result
        | sec_5               | sec_05              | sec_55              |
        | 2026-06-15 14:30:05 | 2026-06-15 14:30:05 | 2026-06-15 14:30:55 |

  Rule: Lenient parsing behavior

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with extra whitespace
      When query
        """
        SELECT
          to_timestamp('2026-06-15  14:30:45', 'yyyy-MM-dd HH:mm:ss') AS extra_space,
          to_timestamp('  2026-06-15 14:30:45  ', 'yyyy-MM-dd HH:mm:ss') AS leading_trailing
        """
      Then query result
        | extra_space         | leading_trailing    |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with case-insensitive month names
      When query
        """
        SELECT
          to_timestamp('2026-june-15', 'yyyy-MMMM-dd') AS lower_june,
          to_timestamp('2026-JUNE-15', 'yyyy-MMMM-dd') AS upper_june,
          to_timestamp('2026-JuNe-15', 'yyyy-MMMM-dd') AS mixed_june
        """
      Then query result
        | lower_june          | upper_june          | mixed_june          |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses with case-insensitive day names
      When query
        """
        SELECT
          to_timestamp('monday, 15 June 2026', 'EEEE, dd MMMM yyyy') AS lower_mon,
          to_timestamp('MONDAY, 15 June 2026', 'EEEE, dd MMMM yyyy') AS upper_mon
        """
      Then query result
        | lower_mon           | upper_mon           |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

  Rule: Parsing with different timezones

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with timezone offset
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45+02:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 12:30:45 |

    Scenario: `to_timestamp` parses with UTC timezone
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45Z', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with negative timezone offset
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45-05:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 19:30:45 |

  Rule: Optional section parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with optional time section
      When query
        """
        SELECT
          to_timestamp('2026-06-15', 'yyyy-MM-dd[ HH:mm:ss]') AS without_time,
          to_timestamp('2026-06-15 14:30:45', 'yyyy-MM-dd[ HH:mm:ss]') AS with_time
        """
      Then query result
        | without_time        | with_time            |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with optional fractional seconds
      When query
        """
        SELECT
          to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd'T'HH:mm:ss[.SSS]") AS without_frac,
          to_timestamp('2026-06-15T14:30:45.123', "yyyy-MM-dd'T'HH:mm:ss[.SSS]") AS with_frac
        """
      Then query result
        | without_frac        | with_frac               |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.123 |

    Scenario: `to_timestamp` parses with nested optional sections
      When query
        """
        SELECT
          to_timestamp('2026-06-15', "yyyy-MM-dd['T'HH:mm:ss[.SSS]]") AS date_only,
          to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd['T'HH:mm:ss[.SSS]]") AS with_time,
          to_timestamp('2026-06-15T14:30:45.789', "yyyy-MM-dd['T'HH:mm:ss[.SSS]]") AS with_frac
        """
      Then query result
        | date_only           | with_time            | with_frac               |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.789 |

    Scenario: `to_timestamp` parses with optional timezone
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45', 'yyyy-MM-dd HH:mm:ss[ XXX]') AS without_tz,
          to_timestamp('2026-06-15 14:30:45+02:00', 'yyyy-MM-dd HH:mm:ss[ XXX]') AS with_tz
        """
      Then query result
        | without_tz          | with_tz              |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

  Rule: Two-digit year parsing (yy and uu)

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses two-digit year with yy (base year 2000)
      When query
        """
        SELECT
          to_timestamp('25-06-15', 'yy-MM-dd') AS year_2025,
          to_timestamp('99-12-31', 'yy-MM-dd') AS year_2099,
          to_timestamp('00-01-01', 'yy-MM-dd') AS year_2000
        """
      Then query result
        | year_2025           | year_2099           | year_2000           |
        | 2025-06-15 00:00:00 | 2099-12-31 00:00:00 | 2000-01-01 00:00:00 |

    Scenario: `to_timestamp` parses two-digit year with uu (base year 2000)
      When query
        """
        SELECT
          to_timestamp('25-06-15', 'uu-MM-dd') AS year_2025,
          to_timestamp('99-12-31', 'uu-MM-dd') AS year_2099,
          to_timestamp('00-01-01', 'uu-MM-dd') AS year_2000
        """
      Then query result
        | year_2025           | year_2099           | year_2000           |
        | 2025-06-15 00:00:00 | 2099-12-31 00:00:00 | 2000-01-01 00:00:00 |

    Scenario: `to_date` parses two-digit year with yy
      When query
        """
        SELECT
          to_date('25-06-15', 'yy-MM-dd') AS year_2025,
          to_date('50-12-31', 'yy-MM-dd') AS year_2050
        """
      Then query result
        | year_2025  | year_2050  |
        | 2025-06-15 | 2050-12-31 |

  Rule: Hour variant parsing (h, K, k, H)

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses 12-hour clock with h (1-12)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 1:30:45 PM', 'yyyy-MM-dd h:mm:ss a') AS hour_1,
          to_timestamp('2026-06-15 12:30:45 PM', 'yyyy-MM-dd h:mm:ss a') AS hour_12,
          to_timestamp('2026-06-15 11:59:59 PM', 'yyyy-MM-dd h:mm:ss a') AS hour_11_pm
        """
      Then query result
        | hour_1              | hour_12             | hour_11_pm           |
        | 2026-06-15 13:30:45 | 2026-06-15 12:30:45 | 2026-06-15 23:59:59 |

    Scenario: `to_timestamp` parses 11-hour clock with K (0-11)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 0:30:45 PM', 'yyyy-MM-dd K:mm:ss a') AS hour_0,
          to_timestamp('2026-06-15 11:30:45 PM', 'yyyy-MM-dd K:mm:ss a') AS hour_11
        """
      Then query result
        | hour_0              | hour_11             |
        | 2026-06-15 12:30:45 | 2026-06-15 23:30:45 |

    Scenario: `to_timestamp` parses 24-hour clock with k (1-24)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 1:30:45', 'yyyy-MM-dd k:mm:ss') AS hour_1,
          to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd k:mm:ss') AS hour_24
        """
      Then query result
        | hour_1              | hour_24             |
        | 2026-06-15 01:30:45 | 2026-06-16 00:00:00 |

    Scenario: `to_timestamp` parses 23-hour clock with H (0-23)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 0:30:45', 'yyyy-MM-dd H:mm:ss') AS hour_0,
          to_timestamp('2026-06-15 23:59:59', 'yyyy-MM-dd H:mm:ss') AS hour_23
        """
      Then query result
        | hour_0              | hour_23             |
        | 2026-06-15 00:30:45 | 2026-06-15 23:59:59 |

  Rule: Width variation parsing for month patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses month with width 1 (M)
      When query
        """
        SELECT
          to_timestamp('2026-6-15', 'yyyy-M-dd') AS month_6,
          to_timestamp('2026-12-15', 'yyyy-M-dd') AS month_12
        """
      Then query result
        | month_6             | month_12            |
        | 2026-06-15 00:00:00 | 2026-12-15 00:00:00 |

    Scenario: `to_timestamp` parses month with width 2 (MM)
      When query
        """
        SELECT
          to_timestamp('2026-06-15', 'yyyy-MM-dd') AS month_06,
          to_timestamp('2026-12-15', 'yyyy-MM-dd') AS month_12
        """
      Then query result
        | month_06            | month_12            |
        | 2026-06-15 00:00:00 | 2026-12-15 00:00:00 |

    Scenario: `to_timestamp` parses month with width 3 (MMM)
      When query
        """
        SELECT
          to_timestamp('2026-Jun-15', 'yyyy-MMM-dd') AS month_jun,
          to_timestamp('2026-Dec-15', 'yyyy-MMM-dd') AS month_dec
        """
      Then query result
        | month_jun           | month_dec           |
        | 2026-06-15 00:00:00 | 2026-12-15 00:00:00 |

    Scenario: `to_timestamp` parses month with width 4 (MMMM)
      When query
        """
        SELECT
          to_timestamp('2026-June-15', 'yyyy-MMMM-dd') AS month_june,
          to_timestamp('2026-December-15', 'yyyy-MMMM-dd') AS month_december
        """
      Then query result
        | month_june          | month_december      |
        | 2026-06-15 00:00:00 | 2026-12-15 00:00:00 |

  Rule: Zone and offset parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses zone offset with X (Z for zero)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45Z', 'yyyy-MM-dd HH:mm:ssX') AS offset_z,
          to_timestamp('2026-06-15 14:30:45+02', 'yyyy-MM-dd HH:mm:ssX') AS offset_02
        """
      Then query result
        | offset_z            | offset_02           |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

    Scenario: `to_timestamp` parses zone offset with x (+00 for zero)
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45+00', 'yyyy-MM-dd HH:mm:ssx') AS offset_00,
          to_timestamp('2026-06-15 14:30:45+02', 'yyyy-MM-dd HH:mm:ssx') AS offset_02
        """
      Then query result
        | offset_00           | offset_02           |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

    Scenario: `to_timestamp` parses zone offset with Z
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45+0000', 'yyyy-MM-dd HH:mm:ssZ') AS offset_0000,
          to_timestamp('2026-06-15 14:30:45+0200', 'yyyy-MM-dd HH:mm:ssZ') AS offset_0200
        """
      Then query result
        | offset_0000         | offset_0200         |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

  Rule: Edge case parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses leap year February 29
      When query
        """
        SELECT
          to_timestamp('2024-02-29', 'yyyy-MM-dd') AS leap_2024,
          to_timestamp('2020-02-29', 'yyyy-MM-dd') AS leap_2020,
          to_timestamp('2000-02-29', 'yyyy-MM-dd') AS leap_2000
        """
      Then query result
        | leap_2024           | leap_2020           | leap_2000           |
        | 2024-02-29 00:00:00 | 2020-02-29 00:00:00 | 2000-02-29 00:00:00 |

    Scenario: `to_timestamp` parses year boundaries
      When query
        """
        SELECT
          to_timestamp('0001-01-01', 'yyyy-MM-dd') AS year_0001,
          to_timestamp('9999-12-31', 'yyyy-MM-dd') AS year_9999
        """
      Then query result
        | year_0001           | year_9999           |
        | 0001-01-01 00:00:00 | 9999-12-31 00:00:00 |

    Scenario: `to_timestamp` parses midnight and noon
      When query
        """
        SELECT
          to_timestamp('2026-06-15 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS midnight,
          to_timestamp('2026-06-15 12:00:00', 'yyyy-MM-dd HH:mm:ss') AS noon
        """
      Then query result
        | midnight            | noon                |
        | 2026-06-15 00:00:00 | 2026-06-15 12:00:00 |

    Scenario: `to_timestamp` parses day of year
      When query
        """
        SELECT
          to_timestamp('2026-166', 'yyyy-DDD') AS day_166,
          to_timestamp('2026-001', 'yyyy-DDD') AS day_001,
          to_timestamp('2026-365', 'yyyy-DDD') AS day_365
        """
      Then query result
        | day_166             | day_001             | day_365             |
        | 2026-06-15 00:00:00 | 2026-01-01 00:00:00 | 2026-12-31 00:00:00 |

    Scenario: `to_timestamp` parses week-based date
      When query
        """
        SELECT
          to_timestamp('2026-W25-1', 'YYYY-\'W\'ww-e') AS week_date,
          to_timestamp('2026-W01-1', 'YYYY-\'W\'ww-e') AS first_week,
          to_timestamp('2026-W52-7', 'YYYY-\'W\'ww-e') AS last_week
        """
      Then query result
        | week_date           | first_week          | last_week            |
        | 2026-06-15 00:00:00 | 2025-12-29 00:00:00 | 2026-12-27 00:00:00 |

  Rule: Week-based pattern variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses week year with YYYY
      When query
        """
        SELECT
          to_timestamp('2025-W01-1', 'YYYY-\'W\'ww-e') AS week_year_2025,
          to_timestamp('2026-W01-1', 'YYYY-\'W\'ww-e') AS week_year_2026
        """
      Then query result
        | week_year_2025      | week_year_2026      |
        | 2024-12-30 00:00:00 | 2025-12-29 00:00:00 |

    Scenario: `to_timestamp` parses week of month with W
      When query
        """
        SELECT
          to_timestamp('2026-06-3', 'yyyy-MM-W') AS week_3,
          to_timestamp('2026-06-1', 'yyyy-MM-W') AS week_1
        """
      Then query result
        | week_3              | week_1              |
        | 2026-06-15 00:00:00 | 2026-06-01 00:00:00 |

    Scenario: `to_timestamp` parses day of week with e
      When query
        """
        SELECT
          to_timestamp('2026-W25-1', 'YYYY-\'W\'ww-e') AS day_1,
          to_timestamp('2026-W25-7', 'YYYY-\'W\'ww-e') AS day_7
        """
      Then query result
        | day_1               | day_7               |
        | 2026-06-15 00:00:00 | 2026-06-21 00:00:00 |

  Rule: Invalid input handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` errors on invalid date
      When query
        """
        SELECT to_timestamp('2026-13-01', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid day
      When query
        """
        SELECT to_timestamp('2026-06-32', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid hour
      When query
        """
        SELECT to_timestamp('2026-06-15 25:00:00', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid minute
      When query
        """
        SELECT to_timestamp('2026-06-15 14:60:00', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid second
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:60', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid month name
      When query
        """
        SELECT to_timestamp('2026-InvalidMonth-15', 'yyyy-MMMM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on non-leap year Feb 29
      When query
        """
        SELECT to_timestamp('2023-02-29', 'yyyy-MM-dd')
        """
      Then query error

  Rule: Timezone parsing variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses timezone name with z
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45 UTC', 'yyyy-MM-dd HH:mm:ss z') AS tz_utc,
          to_timestamp('2026-06-15 14:30:45 GMT', 'yyyy-MM-dd HH:mm:ss z') AS tz_gmt
        """
      Then query result
        | tz_utc              | tz_gmt              |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses timezone ID with VV
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45 America/New_York', 'yyyy-MM-dd HH:mm:ss VV') AS tz_ny,
          to_timestamp('2026-06-15 14:30:45 Europe/London', 'yyyy-MM-dd HH:mm:ss VV') AS tz_london
        """
      Then query result
        | tz_ny               | tz_london           |
        | 2026-06-15 18:30:45 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses localized offset with O
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45 GMT', 'yyyy-MM-dd HH:mm:ss O') AS offset_gmt,
          to_timestamp('2026-06-15 14:30:45 GMT+02:00', 'yyyy-MM-dd HH:mm:ss OOOO') AS offset_02
        """
      Then query result
        | offset_gmt          | offset_02           |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

  Rule: Millisecond and microsecond parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses milliseconds with S
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS ms_123,
          to_timestamp('2026-06-15 14:30:45.999', 'yyyy-MM-dd HH:mm:ss.SSS') AS ms_999
        """
      Then query result
        | ms_123                 | ms_999                 |
        | 2026-06-15 14:30:45.123 | 2026-06-15 14:30:45.999 |

    Scenario: `to_timestamp` parses microseconds with SSSSSS
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS us_123456,
          to_timestamp('2026-06-15 14:30:45.999999', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS us_999999
        """
      Then query result
        | us_123456                  | us_999999                  |
        | 2026-06-15 14:30:45.123456 | 2026-06-15 14:30:45.999999 |

    Scenario: `to_timestamp` parses nanoseconds with SSSSSSSSS
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS ns_123456789
        """
      Then query result
        | ns_123456789               |
        | 2026-06-15 14:30:45.123456 |

  Rule: Multiple format patterns in single query

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses multiple formats in same query
      When query
        """
        SELECT
          to_timestamp('2026-06-15', 'yyyy-MM-dd') AS format1,
          to_timestamp('06/15/2026', 'MM/dd/yyyy') AS format2,
          to_timestamp('15.06.2026', 'dd.MM.yyyy') AS format3
        """
      Then query result
        | format1             | format2             | format3             |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_date` parses multiple formats in same query
      When query
        """
        SELECT
          to_date('2026-06-15', 'yyyy-MM-dd') AS format1,
          to_date('06/15/2026', 'MM/dd/yyyy') AS format2,
          to_date('15.06.2026', 'dd.MM.yyyy') AS format3
        """
      Then query result
        | format1    | format2    | format3    |
        | 2026-06-15 | 2026-06-15 | 2026-06-15 |

  Rule: Special date values

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses epoch
      When query
        """
        SELECT
          to_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS epoch
        """
      Then query result
        | epoch               |
        | 1970-01-01 00:00:00 |

    Scenario: `to_timestamp` parses far future date
      When query
        """
        SELECT
          to_timestamp('9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS far_future
        """
      Then query result
        | far_future          |
        | 9999-12-31 23:59:59 |

    Scenario: `to_timestamp` parses far past date
      When query
        """
        SELECT
          to_timestamp('0001-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS far_past
        """
      Then query result
        | far_past            |
        | 0001-01-01 00:00:00 |

  Rule: Mixed width patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with mixed width patterns
      When query
        """
        SELECT
          to_timestamp('2026-6-5 9:8:7', 'yyyy-M-d H:m:s') AS single_width,
          to_timestamp('2026-06-05 09:08:07', 'yyyy-MM-dd HH:mm:ss') AS double_width,
          to_timestamp('2026-06-05 9:8:7', 'yyyy-MM-dd H:m:s') AS mixed_width
        """
      Then query result
        | single_width        | double_width        | mixed_width         |
        | 2026-06-05 09:08:07 | 2026-06-05 09:08:07 | 2026-06-05 09:08:07 |

    Scenario: `to_timestamp` parses with text and numeric mix
      When query
        """
        SELECT
          to_timestamp('June 5, 2026', 'MMMM d, yyyy') AS text_month,
          to_timestamp('06/05/2026', 'MM/dd/yyyy') AS num_month
        """
      Then query result
        | text_month          | num_month           |
        | 2026-06-05 00:00:00 | 2026-06-05 00:00:00 |

  Rule: Adjacent value parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses adjacent numeric values
      When query
        """
        SELECT
          to_timestamp('20260615', 'yyyyMMdd') AS adjacent_date,
          to_timestamp('20260615143045', 'yyyyMMddHHmmss') AS adjacent_datetime
        """
      Then query result
        | adjacent_date       | adjacent_datetime   |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses adjacent with fractional seconds
      When query
        """
        SELECT
          to_timestamp('20260615143045123', 'yyyyMMddHHmmssSSS') AS adjacent_ms,
          to_timestamp('20260615143045123456', 'yyyyMMddHHmmssSSSSSS') AS adjacent_us
        """
      Then query result
        | adjacent_ms            | adjacent_us               |
        | 2026-06-15 14:30:45.123 | 2026-06-15 14:30:45.123456 |

  Rule: Spark-specific deviation tests

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` handles year 0 differently than Java
      When query
        """
        SELECT to_timestamp('0000-01-01', 'yyyy-MM-dd') AS year_0
        """
      Then query result
        | year_0              |
        | 0000-01-01 00:00:00 |

    Scenario: `to_timestamp` handles negative years
      When query
        """
        SELECT to_timestamp('-0001-01-01', 'yyyy-MM-dd') AS negative_year
        """
      Then query result
        | negative_year       |
        | -0001-01-01 00:00:00 |

    Scenario: `to_timestamp` parses ISO 8601 with timezone designator
      When query
        """
        SELECT
          to_timestamp('2026-06-15T14:30:45Z', "yyyy-MM-dd'T'HH:mm:ss'Z'") AS iso_z,
          to_timestamp('2026-06-15T14:30:45+00:00', "yyyy-MM-dd'T'HH:mm:ssXXX") AS iso_offset
        """
      Then query result
        | iso_z               | iso_offset          |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` handles timezone offset without colon
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45+0000', 'yyyy-MM-dd HH:mm:ssZ') AS offset_no_colon,
          to_timestamp('2026-06-15 14:30:45+0200', 'yyyy-MM-dd HH:mm:ssZ') AS offset_0200
        """
      Then query result
        | offset_no_colon     | offset_0200         |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

    Scenario: `to_timestamp` handles timezone offset with seconds
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45+00:00:00', 'yyyy-MM-dd HH:mm:ssZZZZZ') AS offset_with_sec
        """
      Then query result
        | offset_with_sec     |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with localized date patterns
      When query
        """
        SELECT
          to_timestamp('06/15/2026', 'MM/dd/yyyy') AS us_date,
          to_timestamp('15/06/2026', 'dd/MM/yyyy') AS eu_date,
          to_timestamp('2026/06/15', 'yyyy/MM/dd') AS iso_date
        """
      Then query result
        | us_date             | eu_date             | iso_date            |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` handles 24:00:00 as next day midnight
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd HH:mm:ss') AS midnight_next
        """
      Then query result
        | midnight_next       |
        | 2026-06-16 00:00:00 |

    Scenario: `to_timestamp` handles leap second 23:59:60
      When query
        """
        SELECT to_timestamp('2026-06-15 23:59:60', 'yyyy-MM-dd HH:mm:ss') AS leap_second
        """
      Then query result
        | leap_second         |
        | 2026-06-16 00:00:00 |

    Scenario: `to_timestamp` parses with Thai Buddhist calendar
      When query
        """
        SELECT to_timestamp('2569-06-15', 'yyyy-MM-dd') AS thai_year
        """
      Then query result
        | thai_year           |
        | 2569-06-15 00:00:00 |

    Scenario: `to_timestamp` handles empty optional section
      When query
        """
        SELECT
          to_timestamp('2026-06-15', 'yyyy-MM-dd[ HH:mm:ss][.SSS]') AS empty_optional,
          to_timestamp('2026-06-15 14:30:45', 'yyyy-MM-dd[ HH:mm:ss][.SSS]') AS with_time,
          to_timestamp('2026-06-15 14:30:45.123', 'yyyy-MM-dd[ HH:mm:ss][.SSS]') AS with_frac
        """
      Then query result
        | empty_optional      | with_time            | with_frac               |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.123 |

    Scenario: `to_timestamp` handles multiple consecutive optional sections
      When query
        """
        SELECT
          to_timestamp('2026-06-15', 'yyyy-MM-dd[ T][HH][:mm][:ss][.SSS]') AS minimal,
          to_timestamp('2026-06-15T14', 'yyyy-MM-dd[ T][HH][:mm][:ss][.SSS]') AS with_hour,
          to_timestamp('2026-06-15T14:30', 'yyyy-MM-dd[ T][HH][:mm][:ss][.SSS]') AS with_min,
          to_timestamp('2026-06-15T14:30:45', 'yyyy-MM-dd[ T][HH][:mm][:ss][.SSS]') AS with_sec
        """
      Then query result
        | minimal             | with_hour           | with_min            | with_sec            |
        | 2026-06-15 00:00:00 | 2026-06-15 14:00:00 | 2026-06-15 14:30:00 | 2026-06-15 14:30:45 |

  Rule: Performance and stress tests

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses many timestamps efficiently
      When query
        """
        SELECT
          to_timestamp('2026-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS t1,
          to_timestamp('2026-02-15 12:30:45', 'yyyy-MM-dd HH:mm:ss') AS t2,
          to_timestamp('2026-03-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS t3,
          to_timestamp('2026-04-10 06:15:30', 'yyyy-MM-dd HH:mm:ss') AS t4,
          to_timestamp('2026-05-20 18:45:00', 'yyyy-MM-dd HH:mm:ss') AS t5
        """
      Then query result
        | t1                  | t2                  | t3                  | t4                  | t5                  |
        | 2026-01-01 00:00:00 | 2026-02-15 12:30:45 | 2026-03-31 23:59:59 | 2026-04-10 06:15:30 | 2026-05-20 18:45:00 |

    Scenario: `to_timestamp` parses with complex nested patterns
      When query
        """
        SELECT
          to_timestamp('2026-06-15', "yyyy-MM-dd['T'HH:mm[:ss][.SSS]]") AS level1,
          to_timestamp('2026-06-15T14:30', "yyyy-MM-dd['T'HH:mm[:ss][.SSS]]") AS level2,
          to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd['T'HH:mm[:ss][.SSS]]") AS level3,
          to_timestamp('2026-06-15T14:30:45.789', "yyyy-MM-dd['T'HH:mm[:ss][.SSS]]") AS level4
        """
      Then query result
        | level1              | level2              | level3              | level4               |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.789 |

  Rule: Fractional seconds parsing variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses fractional seconds with varying widths
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1', 'yyyy-MM-dd HH:mm:ss.S') AS frac_1,
          to_timestamp('2026-06-15 14:30:45.12', 'yyyy-MM-dd HH:mm:ss.SS') AS frac_2,
          to_timestamp('2026-06-15 14:30:45.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS frac_3
        """
      Then query result
        | frac_1              | frac_2               | frac_3               |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses fractional seconds with 4-6 digits
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1234', 'yyyy-MM-dd HH:mm:ss.SSSS') AS frac_4,
          to_timestamp('2026-06-15 14:30:45.12345', 'yyyy-MM-dd HH:mm:ss.SSSSS') AS frac_5,
          to_timestamp('2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS frac_6
        """
      Then query result
        | frac_4                 | frac_5                  | frac_6                  |
        | 2026-06-15 14:30:45.1 | 2026-06-15 14:30:45.12 | 2026-06-15 14:30:45.123 |

    Scenario: `to_timestamp` parses fractional seconds with 7-9 digits
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1234567', 'yyyy-MM-dd HH:mm:ss.SSSSSSS') AS frac_7,
          to_timestamp('2026-06-15 14:30:45.12345678', 'yyyy-MM-dd HH:mm:ss.SSSSSSSS') AS frac_8,
          to_timestamp('2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS frac_9
        """
      Then query result
        | frac_7                    | frac_8                     | frac_9                      |
        | 2026-06-15 14:30:45.123 | 2026-06-15 14:30:45.1234 | 2026-06-15 14:30:45.123456 |

  Rule: Escaped literals and special characters

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with escaped single quote
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd'T'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with double single quote
      When query
        """
        SELECT to_timestamp('2026''s year', "yyyy''s year") AS result
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: `to_timestamp` parses with multiple literals
      When query
        """
        SELECT to_timestamp('Date: 2026-06-15 Time: 14:30:45', "'Date: 'yyyy-MM-dd' Time: 'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

  Rule: Different separators and delimiters

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses with slash separator
      When query
        """
        SELECT
          to_timestamp('2026/06/15', 'yyyy/MM/dd') AS date_slash,
          to_timestamp('06/15/2026', 'MM/dd/yyyy') AS us_date
        """
      Then query result
        | date_slash          | us_date             |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses with dot separator
      When query
        """
        SELECT
          to_timestamp('2026.06.15', 'yyyy.MM.dd') AS date_dot,
          to_timestamp('15.06.2026', 'dd.MM.yyyy') AS eu_date
        """
      Then query result
        | date_dot            | eu_date             |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses with no separator
      When query
        """
        SELECT
          to_timestamp('20260615', 'yyyyMMdd') AS date_no_sep,
          to_timestamp('20260615143045', 'yyyyMMddHHmmss') AS datetime_no_sep
        """
      Then query result
        | date_no_sep         | datetime_no_sep     |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with colon in time
      When query
        """
        SELECT
          to_timestamp('14:30:45', 'HH:mm:ss') AS time_colon,
          to_timestamp('14.30.45', 'HH.mm.ss') AS time_dot
        """
      Then query result
        | time_colon          | time_dot            |
        | 1970-01-01 14:30:45 | 1970-01-01 14:30:45 |

    Scenario: `to_timestamp` parses with mixed separators
      When query
        """
        SELECT
          to_timestamp('2026/06-15', 'yyyy/MM-dd') AS mixed_sep,
          to_timestamp('2026-06/15 14.30:45', 'yyyy-MM/dd HH.mm:ss') AS complex_mixed
        """
      Then query result
        | mixed_sep           | complex_mixed       |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 |

  Rule: Standalone month parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses standalone month with L
      When query
        """
        SELECT
          to_timestamp('2026-06', 'yyyy-LL') AS month_06,
          to_timestamp('2026-12', 'yyyy-LL') AS month_12
        """
      Then query result
        | month_06            | month_12            |
        | 2026-06-01 00:00:00 | 2026-12-01 00:00:00 |

    Scenario: `to_timestamp` parses standalone month name with LLL
      When query
        """
        SELECT
          to_timestamp('2026-Jun', 'yyyy-LLL') AS month_jun,
          to_timestamp('2026-Dec', 'yyyy-LLL') AS month_dec
        """
      Then query result
        | month_jun           | month_dec           |
        | 2026-06-01 00:00:00 | 2026-12-01 00:00:00 |

    Scenario: `to_timestamp` parses standalone full month name with LLLL
      When query
        """
        SELECT
          to_timestamp('2026-June', 'yyyy-LLLL') AS month_june,
          to_timestamp('2026-December', 'yyyy-LLLL') AS month_december
        """
      Then query result
        | month_june          | month_december      |
        | 2026-06-01 00:00:00 | 2026-12-01 00:00:00 |

  Rule: Extreme date and time value parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses minimum date (year 0001)
      When query
        """
        SELECT to_timestamp('0001-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 0001-01-01 00:00:00 |

    Scenario: `to_timestamp` parses maximum date (year 9999)
      When query
        """
        SELECT to_timestamp('9999-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 00:00:00 |

    Scenario: `to_timestamp` parses minimum timestamp (year 0001 with time)
      When query
        """
        SELECT to_timestamp('0001-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 0001-01-01 00:00:00 |

    Scenario: `to_timestamp` parses maximum timestamp (year 9999 with time)
      When query
        """
        SELECT to_timestamp('9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

    Scenario: `to_timestamp` parses timestamp with maximum nanoseconds
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                       |
        | 2026-06-15 14:30:45.999999999 |

    Scenario: `to_timestamp` parses timestamp with minimum nanoseconds
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                       |
        | 2026-06-15 14:30:45.000000001 |

    Scenario: `to_timestamp` parses year 2038 boundary (32-bit overflow)
      When query
        """
        SELECT
          to_timestamp('2038-01-19 03:14:07', 'yyyy-MM-dd HH:mm:ss') AS before_overflow,
          to_timestamp('2038-01-19 03:14:08', 'yyyy-MM-dd HH:mm:ss') AS at_overflow
        """
      Then query result
        | before_overflow      | at_overflow           |
        | 2038-01-19 03:14:07 | 2038-01-19 03:14:08 |

    Scenario: `to_timestamp` parses negative Unix epoch (before 1970)
      When query
        """
        SELECT
          to_timestamp('1969-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS before_epoch,
          to_timestamp('1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS year_1900
        """
      Then query result
        | before_epoch          | year_1900           |
        | 1969-12-31 23:59:59 | 1900-01-01 00:00:00 |

    Scenario: `to_timestamp` parses leap year century (2000)
      When query
        """
        SELECT to_timestamp('2000-02-29', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 2000-02-29 00:00:00 |

    Scenario: `to_timestamp` errors on non-leap century Feb 29 (1900)
      When query
        """
        SELECT to_timestamp('1900-02-29', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` parses first day of each month
      When query
        """
        SELECT
          to_timestamp('2026-01-01', 'yyyy-MM-dd') AS jan,
          to_timestamp('2026-02-01', 'yyyy-MM-dd') AS feb,
          to_timestamp('2026-03-01', 'yyyy-MM-dd') AS mar,
          to_timestamp('2026-04-01', 'yyyy-MM-dd') AS apr,
          to_timestamp('2026-05-01', 'yyyy-MM-dd') AS may,
          to_timestamp('2026-06-01', 'yyyy-MM-dd') AS jun,
          to_timestamp('2026-07-01', 'yyyy-MM-dd') AS jul,
          to_timestamp('2026-08-01', 'yyyy-MM-dd') AS aug,
          to_timestamp('2026-09-01', 'yyyy-MM-dd') AS sep,
          to_timestamp('2026-10-01', 'yyyy-MM-dd') AS oct,
          to_timestamp('2026-11-01', 'yyyy-MM-dd') AS nov,
          to_timestamp('2026-12-01', 'yyyy-MM-dd') AS dec
        """
      Then query result
        | jan                 | feb                 | mar                 | apr                 | may                 | jun                 | jul                 | aug                 | sep                 | oct                 | nov                 | dec                 |
        | 2026-01-01 00:00:00 | 2026-02-01 00:00:00 | 2026-03-01 00:00:00 | 2026-04-01 00:00:00 | 2026-05-01 00:00:00 | 2026-06-01 00:00:00 | 2026-07-01 00:00:00 | 2026-08-01 00:00:00 | 2026-09-01 00:00:00 | 2026-10-01 00:00:00 | 2026-11-01 00:00:00 | 2026-12-01 00:00:00 |

    Scenario: `to_timestamp` parses last day of each month
      When query
        """
        SELECT
          to_timestamp('2026-01-31', 'yyyy-MM-dd') AS jan,
          to_timestamp('2026-02-28', 'yyyy-MM-dd') AS feb,
          to_timestamp('2026-03-31', 'yyyy-MM-dd') AS mar,
          to_timestamp('2026-04-30', 'yyyy-MM-dd') AS apr,
          to_timestamp('2026-05-31', 'yyyy-MM-dd') AS may,
          to_timestamp('2026-06-30', 'yyyy-MM-dd') AS jun,
          to_timestamp('2026-07-31', 'yyyy-MM-dd') AS jul,
          to_timestamp('2026-08-31', 'yyyy-MM-dd') AS aug,
          to_timestamp('2026-09-30', 'yyyy-MM-dd') AS sep,
          to_timestamp('2026-10-31', 'yyyy-MM-dd') AS oct,
          to_timestamp('2026-11-30', 'yyyy-MM-dd') AS nov,
          to_timestamp('2026-12-31', 'yyyy-MM-dd') AS dec
        """
      Then query result
        | jan                 | feb                 | mar                 | apr                 | may                 | jun                 | jul                 | aug                 | sep                 | oct                 | nov                 | dec                 |
        | 2026-01-31 00:00:00 | 2026-02-28 00:00:00 | 2026-03-31 00:00:00 | 2026-04-30 00:00:00 | 2026-05-31 00:00:00 | 2026-06-30 00:00:00 | 2026-07-31 00:00:00 | 2026-08-31 00:00:00 | 2026-09-30 00:00:00 | 2026-10-31 00:00:00 | 2026-11-30 00:00:00 | 2026-12-31 00:00:00 |

    Scenario: `to_timestamp` parses last day of February in leap year
      When query
        """
        SELECT
          to_timestamp('2024-02-29', 'yyyy-MM-dd') AS leap_2024,
          to_timestamp('2020-02-29', 'yyyy-MM-dd') AS leap_2020,
          to_timestamp('2000-02-29', 'yyyy-MM-dd') AS leap_2000
        """
      Then query result
        | leap_2024           | leap_2020           | leap_2000           |
        | 2024-02-29 00:00:00 | 2020-02-29 00:00:00 | 2000-02-29 00:00:00 |

    Scenario: `to_timestamp` parses time at midnight boundary
      When query
        """
        SELECT
          to_timestamp('2026-06-15 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS midnight,
          to_timestamp('2026-06-15 00:00:00.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS midnight_nano
        """
      Then query result
        | midnight            | midnight_nano                 |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00.000000001 |

    Scenario: `to_timestamp` parses time at last second of day
      When query
        """
        SELECT
          to_timestamp('2026-06-15 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS last_second,
          to_timestamp('2026-06-15 23:59:59.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS last_nano
        """
      Then query result
        | last_second          | last_nano                      |
        | 2026-06-15 23:59:59 | 2026-06-15 23:59:59.999999999 |

    Scenario: `to_timestamp` errors on invalid day for 30-day month
      When query
        """
        SELECT to_timestamp('2026-04-31', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid day for 31-day month
      When query
        """
        SELECT to_timestamp('2026-01-32', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on Feb 30
      When query
        """
        SELECT to_timestamp('2026-02-30', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on Feb 31
      When query
        """
        SELECT to_timestamp('2026-02-31', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid month 00
      When query
        """
        SELECT to_timestamp('2026-00-15', 'yyyy-MM-dd')
        """
      Then query error

    Scenario: `to_timestamp` errors on invalid month 13
      When query
        """
        SELECT to_timestamp('2026-13-15', 'yyyy-MM-dd')
        """
      Then query error

  Rule: Locale-specific parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses French locale date and time
      When query
        """
        SELECT to_timestamp('lundi, 15 juin 2026 2 PM', 'EEEE, dd MMMM yyyy h a', 'fr-FR') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:00:00 |

    Scenario: `to_timestamp` parses Japanese locale date and time
      When query
        """
        SELECT to_timestamp('月曜日, 15 6月 2026 2 午後', 'EEEE, dd MMMM yyyy h a', 'ja-JP') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:00:00 |

    Scenario: `to_timestamp` parses German locale with case-insensitive text
      When query
        """
        SELECT to_timestamp('montag, 15 juni 2026 2 pm', 'EEEE, dd MMMM yyyy h a', 'de-DE') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:00:00 |

  Rule: Aligned week-of-month parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses aligned week-of-month with F
      When query
        """
        SELECT
          to_timestamp('2026-06-15 1', 'yyyy-MM-dd F') AS week_1,
          to_timestamp('2026-06-08 2', 'yyyy-MM-dd F') AS week_2
        """
      Then query result
        | week_1              | week_2              |
        | 2026-06-15 00:00:00 | 2026-06-08 00:00:00 |

    Scenario: `to_timestamp` errors on inconsistent aligned week-of-month
      When query
        """
        SELECT to_timestamp('2026-06-15 2', 'yyyy-MM-dd F')
        """
      Then query error

  Rule: Week-of-month validation

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` validates week-of-month consistency
      When query
        """
        SELECT
          to_timestamp('2026-06-15 3 1', 'yyyy-MM-dd W F') AS valid_week,
          to_timestamp('2026-06-01 1 1', 'yyyy-MM-dd W F') AS first_week
        """
      Then query result
        | valid_week           | first_week          |
        | 2026-06-15 00:00:00 | 2026-06-01 00:00:00 |

    Scenario: `to_timestamp` errors on invalid week-of-month
      When query
        """
        SELECT to_timestamp('2026-06-15 6 1', 'yyyy-MM-dd W F')
        """
      Then query error

    Scenario: `to_timestamp` errors on inconsistent week-of-month
      When query
        """
        SELECT to_timestamp('2026-06-15 2 1', 'yyyy-MM-dd W F')
        """
      Then query error

  Rule: Pattern parsing with optional sections and literals

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses nested optional sections
      When query
        """
        SELECT
          to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd['T'HH:mm[:ss]]") AS with_seconds,
          to_timestamp('2026-06-15T14:30', "yyyy-MM-dd['T'HH:mm[:ss]]") AS without_seconds
        """
      Then query result
        | with_seconds          | without_seconds      |
        | 2026-06-15 14:30:45  | 2026-06-15 14:30:00 |

    Scenario: `to_timestamp` parses quoted literals with special characters
      When query
        """
        SELECT to_timestamp("2026-06-15 'Q'", "yyyy-MM-dd ''Q''") AS result
        """
      Then query result
        | result               |
        | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses pattern with quarter field
      When query
        """
        SELECT to_timestamp('2026-06-15 Q2', "yyyy-MM-dd 'Q'Q") AS result
        """
      Then query result
        | result               |
        | 2026-06-15 00:00:00 |

  Rule: Pattern validation and error handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` rejects invalid pattern width
      When query
        """
        SELECT to_timestamp('2026-06-15', 'MMMMMM')
        """
      Then query error

    Scenario: `to_timestamp` rejects unclosed optional section
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30', 'yyyy-MM-dd[')
        """
      Then query error

    Scenario: `to_timestamp` rejects unexpected closing bracket
      When query
        """
        SELECT to_timestamp('2026-06-15T14:30', 'yyyy-MM-dd]')
        """
      Then query error
