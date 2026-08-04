# Moved from features/datetime_parse.feature by the datetime/ layout reorganisation.
Feature: datetime parsing with format strings

  Rule: Spark datetime pattern validation for parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Predefined formatter name: <case>
      When query
        """
        SELECT <fn>('<in>', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                                          | fn           | in         | fmt                |
        | `to_timestamp` rejects ISO_LOCAL_DATE as a predefined name    | to_timestamp | 2026-06-15 | ISO_LOCAL_DATE     |
        | `to_date` rejects BASIC_ISO_DATE as a predefined name          | to_date      | 2026-06-15 | BASIC_ISO_DATE     |
        | `to_timestamp` rejects ISO_WEEK_DATE as a predefined name      | to_timestamp | 2026-06-15 | ISO_WEEK_DATE      |
        | `to_timestamp` rejects RFC_1123_DATE_TIME as a predefined name | to_timestamp | 2026-06-15 | RFC_1123_DATE_TIME |

    Scenario Outline: Disabled week-based parsing pattern: <case>
      When query
        """
        SELECT to_timestamp('2018-11-17', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                              | fmt |
        | `to_timestamp` rejects week-based year            | Y   |
        | `to_timestamp` rejects week of month              | W   |
        | `to_timestamp` rejects week of year               | w   |
        | `to_timestamp` rejects ISO day number             | u   |
        | `to_timestamp` rejects localized day number       | e   |
        | `to_timestamp` rejects stand-alone day number     | c   |

    Scenario Outline: Parsing-only restricted pattern: <case>
      When query
        """
        SELECT to_timestamp('2018-11-17', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                                  | fmt |
        | `to_timestamp` rejects day-of-week text while parsing | E   |
        | `to_timestamp` rejects aligned day while parsing      | F   |
        | `to_timestamp` rejects stand-alone quarter parsing    | q   |
        | `to_timestamp` rejects quarter parsing                | Q   |

    Scenario Outline: Invalid Java datetime parsing pattern: <case>
      When query
        """
        SELECT to_timestamp('2018-11-17', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                                   | fmt        |
        | `to_timestamp` rejects narrow month                    | MMMMM      |
        | `to_timestamp` rejects year wider than six digits      | yyyyyyy    |
        | `to_timestamp` rejects three 24-hour letters           | HHH        |
        | `to_timestamp` rejects ten fractional-second letters   | SSSSSSSSSS |
        | `to_timestamp` rejects millisecond-of-day              | A          |
        | `to_timestamp` rejects day-period                      | B          |
        | `to_timestamp` rejects nano-of-second                  | n          |
        | `to_timestamp` rejects nano-of-day                     | N          |
        | `to_timestamp` rejects pad-next                        | p          |
        | `to_timestamp` rejects unknown pattern letter C        | C          |
        | `to_timestamp` rejects unknown pattern letter I        | I          |

  Rule: Parsing with custom format patterns

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Custom pattern: <case>
      When query
        """
        SELECT <fn>('<in>', <fmt>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                        | fn               | in                         | fmt                          | result                     |
        | `to_timestamp` parses custom format with literal separator  | to_timestamp     | 2026-06-15T14:30:45        | "yyyy-MM-dd'T'HH:mm:ss"      | 2026-06-15 14:30:45        |
        | `to_timestamp` parses custom format with fractional seconds | to_timestamp     | 2026-06-15 14:30:45.123456 | 'yyyy-MM-dd HH:mm:ss.SSSSSS' | 2026-06-15 14:30:45.123456 |
        | `to_date` parses custom format                              | to_date          | 2026/06/15                 | 'yyyy/MM/dd'                 | 2026-06-15                 |
        | `to_timestamp` parses with month name                       | to_timestamp     | 15 June 2026               | 'dd MMMM yyyy'               | 2026-06-15 00:00:00        |
        | `to_timestamp_ltz` parses custom format with offset         | to_timestamp_ltz | 2026-06-15T16:30:45+02:00  | "yyyy-MM-dd'T'HH:mm:ssXXX"   | 2026-06-15 14:30:45        |
        | `to_timestamp_ntz` ignores a positive offset                | to_timestamp_ntz | 2026-06-15T16:30:45+02:00  | "yyyy-MM-dd'T'HH:mm:ssXXX"   | 2026-06-15 16:30:45        |

  Rule: Formatted TIMESTAMP_NTZ offset handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp_ntz` ignores a negative offset
      When query
        """
        SELECT to_timestamp_ntz(
          '2026-06-15T16:30:45-05:00',
          "yyyy-MM-dd'T'HH:mm:ssXXX"
        ) AS result
        """
      Then query result
        | result              |
        | 2026-06-15 16:30:45 |

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

    Scenario Outline: Strict era parsing accepts width-matched era text: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 00:00:00 |

      Examples:
        | case                              | in                     | fmt             |
        | short AD prefix with G            | AD 2026-06-15          | G yyyy-MM-dd    |
        | short AD suffix with G            | 2026-06-15 AD          | yyyy-MM-dd G    |
        | full Anno Domini prefix with GGGG | Anno Domini 2026-06-15 | GGGG yyyy-MM-dd |

    Scenario Outline: Strict era parsing rejects unsupported or width-mismatched era text: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                         | in                     | fmt             |
        | CE prefix with G             | CE 2026-06-15          | G yyyy-MM-dd    |
        | CE suffix with G             | 2026-06-15 CE          | yyyy-MM-dd G    |
        | full Anno Domini text with G | Anno Domini 2026-06-15 | G yyyy-MM-dd    |
        | short AD text with GGGG      | AD 2026-06-15          | GGGG yyyy-MM-dd |
        | narrow A text with G         | A 2026-06-15           | G yyyy-MM-dd    |

    Scenario Outline: NULL handling: <case>
      When query
        """
        SELECT <fn>(<in>, <fmt>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                               | fn           | in                   | fmt                  |
        | `to_timestamp` handles NULL input  | to_timestamp | CAST(NULL AS STRING) | 'yyyy-MM-dd'         |
        | `to_date` handles NULL input       | to_date      | CAST(NULL AS STRING) | 'yyyy-MM-dd'         |
        | `to_timestamp` handles NULL format | to_timestamp | '2026-06-15'         | CAST(NULL AS STRING) |

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

    Scenario Outline: `to_timestamp` rejects extra whitespace under strict input consumption: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<in>, 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

      Examples:
        | case                                           | in                                        |
        | leading space before the timestamp             | concat(' ', '2026-06-15 14:30:45')        |
        | trailing space after the timestamp             | concat('2026-06-15 14:30:45', ' ')        |
        | double space where the pattern has one space   | '2026-06-15  14:30:45'                    |

    Scenario: `to_timestamp` rejects an unpatterned bracket suffix under strict input consumption
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45[garbage]', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

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

  Rule: Parsing with different timezones

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Timezone offset: <case>
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45<offset>', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | offset | result              |
        | `to_timestamp` parses with timezone offset          | +02:00 | 2026-06-15 12:30:45 |
        | `to_timestamp` parses with UTC timezone             | Z      | 2026-06-15 14:30:45 |
        | `to_timestamp` parses with negative timezone offset | -05:00 | 2026-06-15 19:30:45 |

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
        | without_time        | with_time           |
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
        | date_only           | with_time           | with_frac               |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.789 |

    Scenario: `to_timestamp` parses with optional timezone under strict input consumption
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45', 'yyyy-MM-dd HH:mm:ss[ XXX]') AS without_tz,
          to_timestamp('2026-06-15 14:30:45 +02:00', 'yyyy-MM-dd HH:mm:ss[ XXX]') AS with_tz
        """
      Then query result
        | without_tz          | with_tz             |
        | 2026-06-15 14:30:45 | 2026-06-15 12:30:45 |

    Scenario: `to_timestamp` rejects optional timezone without its literal space under strict input consumption
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45+02:00', 'yyyy-MM-dd HH:mm:ss[ XXX]')
        """
      Then query error .*

  Rule: Two-digit year parsing

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Two-digit year: <case>
      When query
        """
        SELECT
          to_timestamp('25-06-15', '<pat>-MM-dd') AS year_2025,
          to_timestamp('99-12-31', '<pat>-MM-dd') AS year_2099,
          to_timestamp('00-01-01', '<pat>-MM-dd') AS year_2000
        """
      Then query result
        | year_2025           | year_2099           | year_2000           |
        | 2025-06-15 00:00:00 | 2099-12-31 00:00:00 | 2000-01-01 00:00:00 |

      Examples:
        | case                                                          | pat |
        | `to_timestamp` parses two-digit year with yy (base year 2000) | yy  |

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
        | hour_1              | hour_12             | hour_11_pm          |
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
        | 2026-06-15 01:30:45 | 2026-06-15 00:00:00 |

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

    Scenario: `to_timestamp` distinguishes k and H at 24:00:00: k succeeds
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd k:mm:ss') AS clock_hour_24
        """
      Then query result
        | clock_hour_24       |
        | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` distinguishes k and H at 24:00:00: H errors in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

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

    Scenario Outline: Offset pattern semantics parses X and x width <case> to the same instant
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          to_timestamp('<local><upper_offset>', 'yyyy-MM-dd HH:mm:ss<upper>') AS upper_result,
          to_timestamp('<local><lower_offset>', 'yyyy-MM-dd HH:mm:ss<lower>') AS lower_result
        """
      Then query result
        | upper_result        | lower_result        |
        | 2026-06-15 12:00:45 | 2026-06-15 12:00:45 |

      Examples:
        | case                            | local               | upper_offset | upper | lower_offset | lower |
        | 1 zero                          | 2026-06-15 12:00:45 | Z            | X     | +00          | x     |
        | 1 hour only                     | 2026-06-15 14:00:45 | +02          | X     | +02          | x     |
        | 1 with minute                   | 2026-06-15 14:30:45 | +0230        | X     | +0230        | x     |
        | 2                               | 2026-06-15 14:30:45 | +0230        | XX    | +0230        | xx    |
        | 3                               | 2026-06-15 14:30:45 | +02:30       | XXX   | +02:30       | xxx   |
        | 4 with second                   | 2026-06-15 14:31:00 | +023015      | XXXX  | +023015      | xxxx  |
        | 5 with second                   | 2026-06-15 14:31:00 | +02:30:15    | XXXXX | +02:30:15    | xxxxx |

    Scenario Outline: Offset pattern semantics rejects invalid offset components and ranges: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 12:00:45<offset>', 'yyyy-MM-dd HH:mm:ss<fmt>')
        """
      Then query error .*

      Examples:
        | case                              | offset    | fmt   |
        | minute component 60               | +01:60    | XXX   |
        | second component 60               | +01:00:60 | XXXXX |
        | positive offset beyond +18:00      | +19:00    | XXX   |
        | negative offset beyond -18:00      | -18:01    | XXX   |

    Scenario: Offset pattern semantics try_to_timestamp returns NULL for invalid offsets
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          try_to_timestamp('2026-06-15 12:00:45+01:60', 'yyyy-MM-dd HH:mm:ssXXX') AS minute_60,
          try_to_timestamp('2026-06-15 12:00:45+01:00:60', 'yyyy-MM-dd HH:mm:ssXXXXX') AS second_60,
          try_to_timestamp('2026-06-15 12:00:45+19:00', 'yyyy-MM-dd HH:mm:ssXXX') AS hour_19,
          try_to_timestamp('2026-06-15 12:00:45-18:01', 'yyyy-MM-dd HH:mm:ssXXX') AS past_negative_limit
        """
      Then query result
        | minute_60 | second_60 | hour_19 | past_negative_limit |
        | NULL      | NULL      | NULL    | NULL                |

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

  Rule: Invalid input handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Invalid input: <case>
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                          | in                   | fmt                 |
        | `to_timestamp` errors on invalid date         | 2026-13-01           | yyyy-MM-dd          |
        | `to_timestamp` errors on invalid day          | 2026-06-32           | yyyy-MM-dd          |
        | `to_timestamp` errors on invalid hour         | 2026-06-15 25:00:00  | yyyy-MM-dd HH:mm:ss |
        | `to_timestamp` errors on invalid minute       | 2026-06-15 14:60:00  | yyyy-MM-dd HH:mm:ss |
        | `to_timestamp` errors on invalid month name   | 2026-InvalidMonth-15 | yyyy-MMMM-dd        |
        | `to_timestamp` errors on non-leap year Feb 29 | 2023-02-29           | yyyy-MM-dd          |

    Scenario: `to_timestamp` errors on invalid second
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:60', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

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
        | 2026-06-15 18:30:45 | 2026-06-15 13:30:45 |

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
        | ms_123                  | ms_999                  |
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

    Scenario Outline: Multiple formats: <case>
      When query
        """
        SELECT
          <fn>('2026-06-15', 'yyyy-MM-dd') AS format1,
          <fn>('06/15/2026', 'MM/dd/yyyy') AS format2,
          <fn>('15.06.2026', 'dd.MM.yyyy') AS format3
        """
      Then query result
        | format1 | format2 | format3 |
        | <r>     | <r>     | <r>     |

      Examples:
        | case                                                 | fn           | r                   |
        | `to_timestamp` parses multiple formats in same query | to_timestamp | 2026-06-15 00:00:00 |
        | `to_date` parses multiple formats in same query      | to_date      | 2026-06-15          |

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

    Scenario: `to_timestamp` parses a fraction adjacent to fixed-width seconds
      When query
        """
        SELECT
          to_timestamp(
            '20181202.210400123',
            'yyyyMMdd.HHmmssSSS'
          ) AS literal_boundary,
          to_timestamp(
            '260615143045123',
            'yyMMddHHmmssSSS'
          ) AS fixed_run,
          to_timestamp(
            '202606151430451',
            'yyyyMMddHHmmssS'
          ) AS fixed_fraction,
          to_timestamp(
            '20260615.1430451',
            'yyyyMMdd[.HHmmss]S'
          ) AS optional_literal_boundary
        """
      Then query result
        | literal_boundary        | fixed_run               | fixed_fraction         | optional_literal_boundary |
        | 2018-12-02 21:04:00.123 | 2026-06-15 14:30:45.123 | 2026-06-15 14:30:45.1 | 2026-06-15 14:30:45.1 |

    Scenario Outline: `to_timestamp` rejects adjacent fractional seconds under strict input consumption: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                      | in                   | fmt                    |
        | adjacent millisecond fraction with SSS    | 20260615143045123     | yyyyMMddHHmmssSSS      |
        | adjacent microsecond fraction with SSSSSS | 20260615143045123456  | yyyyMMddHHmmssSSSSSS   |
        | present numeric optional before fraction  | 20260615143045123     | yyyyMMdd[HHmmss]SSS    |
        | absent numeric optional before fraction   | 20260615123           | yyyyMMdd[HHmmss]SSS    |
        | numeric optional before fixed fraction     | 202606151430451       | yyyyMMdd[HHmmss]S      |

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
        | negative_year        |
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

    Scenario: `to_timestamp` rejects H=24 at 24:00:00 in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

    Scenario: `to_timestamp` rejects leap second 23:59:60 in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2026-06-15 23:59:60', 'yyyy-MM-dd HH:mm:ss')
        """
      Then query error .*

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
        | empty_optional      | with_time           | with_frac               |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.123 |

    Scenario: `to_timestamp` handles multiple consecutive optional sections
      When query
        """
        SELECT
          to_timestamp('2026-06-15', "yyyy-MM-dd['T'][HH][:mm][:ss][.SSS]") AS minimal,
          to_timestamp('2026-06-15T14', "yyyy-MM-dd['T'][HH][:mm][:ss][.SSS]") AS with_hour,
          to_timestamp('2026-06-15T14:30', "yyyy-MM-dd['T'][HH][:mm][:ss][.SSS]") AS with_min,
          to_timestamp('2026-06-15T14:30:45', "yyyy-MM-dd['T'][HH][:mm][:ss][.SSS]") AS with_sec
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
        | level1              | level2              | level3              | level4                  |
        | 2026-06-15 00:00:00 | 2026-06-15 14:30:00 | 2026-06-15 14:30:45 | 2026-06-15 14:30:45.789 |

  Rule: Fractional seconds parsing variations

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Six-letter fractional pattern: <case>
      When query
        """
        SELECT
          to_timestamp(
            '2026-06-15 14:30:45.<fraction>',
            'yyyy-MM-dd HH:mm:ss.SSSSSS'
          ) AS parsed,
          date_format(
            to_timestamp(
              '2026-06-15 14:30:45.<fraction>',
              'yyyy-MM-dd HH:mm:ss.SSSSSS'
            ),
            'SSSSSS'
          ) AS formatted_fraction
        """
      Then query result
        | parsed   | formatted_fraction   |
        | <parsed> | <formatted_fraction> |

      Examples:
        | case                                                    | fraction | parsed                     | formatted_fraction |
        | `to_timestamp` parses one fractional digit with SSSSSS  | 1        | 2026-06-15 14:30:45.1      | 100000             |
        | `to_timestamp` parses three fractional digits with SSSSSS | 123    | 2026-06-15 14:30:45.123    | 123000             |
        | `to_timestamp` parses six fractional digits with SSSSSS | 123456   | 2026-06-15 14:30:45.123456 | 123456             |

    Scenario: `to_timestamp` parses fractional seconds with varying widths
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1', 'yyyy-MM-dd HH:mm:ss.S') AS frac_1,
          to_timestamp('2026-06-15 14:30:45.12', 'yyyy-MM-dd HH:mm:ss.SS') AS frac_2,
          to_timestamp('2026-06-15 14:30:45.123', 'yyyy-MM-dd HH:mm:ss.SSS') AS frac_3
        """
      Then query result
        | frac_1                | frac_2                 | frac_3                  |
        | 2026-06-15 14:30:45.1 | 2026-06-15 14:30:45.12 | 2026-06-15 14:30:45.123 |

    Scenario: `to_timestamp` parses fractional seconds with 4-6 digits
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1234', 'yyyy-MM-dd HH:mm:ss.SSSS') AS frac_4,
          to_timestamp('2026-06-15 14:30:45.12345', 'yyyy-MM-dd HH:mm:ss.SSSSS') AS frac_5,
          to_timestamp('2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS frac_6
        """
      Then query result
        | frac_4                   | frac_5                    | frac_6                     |
        | 2026-06-15 14:30:45.1234 | 2026-06-15 14:30:45.12345 | 2026-06-15 14:30:45.123456 |

    Scenario: `to_timestamp` parses fractional seconds with 7-9 digits
      When query
        """
        SELECT
          to_timestamp('2026-06-15 14:30:45.1234567', 'yyyy-MM-dd HH:mm:ss.SSSSSSS') AS frac_7,
          to_timestamp('2026-06-15 14:30:45.12345678', 'yyyy-MM-dd HH:mm:ss.SSSSSSSS') AS frac_8,
          to_timestamp('2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS frac_9
        """
      Then query result
        | frac_7                     | frac_8                     | frac_9                     |
        | 2026-06-15 14:30:45.123456 | 2026-06-15 14:30:45.123456 | 2026-06-15 14:30:45.123456 |

  Rule: Escaped literals and special characters

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Escaped literal: <case>
      When query
        """
        SELECT to_timestamp('<in>', <fmt>) AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

      Examples:
        | case                                            | in                              | fmt                                   |
        | `to_timestamp` parses with escaped single quote | 2026-06-15T14:30:45             | "yyyy-MM-dd'T'HH:mm:ss"               |
        | `to_timestamp` parses with multiple literals    | Date: 2026-06-15 Time: 14:30:45 | "'Date: 'yyyy-MM-dd' Time: 'HH:mm:ss" |

    Scenario: `to_timestamp` parses with double single quote
      When query
        """
        SELECT to_timestamp('2026''s year', "yyyy''s year") AS result
        """
      Then query error (?i).*

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

    Scenario Outline: Extreme value: <case>
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                          | in                            | fmt                           | result                     |
        | `to_timestamp` parses minimum date (year 0001)                | 0001-01-01                    | yyyy-MM-dd                    | 0001-01-01 00:00:00        |
        | `to_timestamp` parses maximum date (year 9999)                | 9999-12-31                    | yyyy-MM-dd                    | 9999-12-31 00:00:00        |
        | `to_timestamp` parses minimum timestamp (year 0001 with time) | 0001-01-01 00:00:00           | yyyy-MM-dd HH:mm:ss           | 0001-01-01 00:00:00        |
        | `to_timestamp` parses maximum timestamp (year 9999 with time) | 9999-12-31 23:59:59           | yyyy-MM-dd HH:mm:ss           | 9999-12-31 23:59:59        |
        | `to_timestamp` parses timestamp with maximum nanoseconds      | 2026-06-15 14:30:45.999999999 | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45.999999 |
        | `to_timestamp` parses timestamp with minimum nanoseconds      | 2026-06-15 14:30:45.000000001 | yyyy-MM-dd HH:mm:ss.SSSSSSSSS | 2026-06-15 14:30:45        |
        | `to_timestamp` parses leap year century (2000)                | 2000-02-29                    | yyyy-MM-dd                    | 2000-02-29 00:00:00        |

    Scenario Outline: Extreme value error: <case>
      When query
        """
        SELECT to_timestamp('<in>', 'yyyy-MM-dd')
        """
      Then query error .*

      Examples:
        | case                                                    | in         |
        | `to_timestamp` errors on non-leap century Feb 29 (1900) | 1900-02-29 |
        | `to_timestamp` errors on invalid day for 30-day month   | 2026-04-31 |
        | `to_timestamp` errors on invalid day for 31-day month   | 2026-01-32 |
        | `to_timestamp` errors on Feb 30                         | 2026-02-30 |
        | `to_timestamp` errors on Feb 31                         | 2026-02-31 |
        | `to_timestamp` errors on invalid month 00               | 2026-00-15 |
        | `to_timestamp` errors on invalid month 13               | 2026-13-15 |

    Scenario Outline: Every month: <case>
      When query
        """
        SELECT
          to_timestamp('2026-01-<jan>', 'yyyy-MM-dd') AS jan,
          to_timestamp('2026-02-<feb>', 'yyyy-MM-dd') AS feb,
          to_timestamp('2026-03-<mar>', 'yyyy-MM-dd') AS mar,
          to_timestamp('2026-04-<apr>', 'yyyy-MM-dd') AS apr,
          to_timestamp('2026-05-<may>', 'yyyy-MM-dd') AS may,
          to_timestamp('2026-06-<jun>', 'yyyy-MM-dd') AS jun,
          to_timestamp('2026-07-<jul>', 'yyyy-MM-dd') AS jul,
          to_timestamp('2026-08-<aug>', 'yyyy-MM-dd') AS aug,
          to_timestamp('2026-09-<sep>', 'yyyy-MM-dd') AS sep,
          to_timestamp('2026-10-<oct>', 'yyyy-MM-dd') AS oct,
          to_timestamp('2026-11-<nov>', 'yyyy-MM-dd') AS nov,
          to_timestamp('2026-12-<dec>', 'yyyy-MM-dd') AS dec
        """
      Then query result
        | jan                    | feb                    | mar                    | apr                    | may                    | jun                    | jul                    | aug                    | sep                    | oct                    | nov                    | dec                    |
        | 2026-01-<jan> 00:00:00 | 2026-02-<feb> 00:00:00 | 2026-03-<mar> 00:00:00 | 2026-04-<apr> 00:00:00 | 2026-05-<may> 00:00:00 | 2026-06-<jun> 00:00:00 | 2026-07-<jul> 00:00:00 | 2026-08-<aug> 00:00:00 | 2026-09-<sep> 00:00:00 | 2026-10-<oct> 00:00:00 | 2026-11-<nov> 00:00:00 | 2026-12-<dec> 00:00:00 |

      Examples:
        | case                                          | jan | feb | mar | apr | may | jun | jul | aug | sep | oct | nov | dec |
        | `to_timestamp` parses first day of each month | 01  | 01  | 01  | 01  | 01  | 01  | 01  | 01  | 01  | 01  | 01  | 01  |
        | `to_timestamp` parses last day of each month  | 31  | 28  | 31  | 30  | 31  | 30  | 31  | 31  | 30  | 31  | 30  | 31  |

    Scenario: `to_timestamp` parses year 2038 boundary (32-bit overflow)
      When query
        """
        SELECT
          to_timestamp('2038-01-19 03:14:07', 'yyyy-MM-dd HH:mm:ss') AS before_overflow,
          to_timestamp('2038-01-19 03:14:08', 'yyyy-MM-dd HH:mm:ss') AS at_overflow
        """
      Then query result
        | before_overflow     | at_overflow         |
        | 2038-01-19 03:14:07 | 2038-01-19 03:14:08 |

    Scenario: `to_timestamp` parses negative Unix epoch (before 1970)
      When query
        """
        SELECT
          to_timestamp('1969-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS before_epoch,
          to_timestamp('1900-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AS year_1900
        """
      Then query result
        | before_epoch        | year_1900           |
        | 1969-12-31 23:59:59 | 1900-01-01 00:00:00 |

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
        | midnight            | midnight_nano       |
        | 2026-06-15 00:00:00 | 2026-06-15 00:00:00 |

    Scenario: `to_timestamp` parses time at last second of day
      When query
        """
        SELECT
          to_timestamp('2026-06-15 23:59:59', 'yyyy-MM-dd HH:mm:ss') AS last_second,
          to_timestamp('2026-06-15 23:59:59.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS last_nano
        """
      Then query result
        | last_second         | last_nano                  |
        | 2026-06-15 23:59:59 | 2026-06-15 23:59:59.999999 |

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
        | with_seconds        | without_seconds     |
        | 2026-06-15 14:30:45 | 2026-06-15 14:30:00 |

    Scenario Outline: Quoted literal: <case>
      When query
        """
        SELECT to_timestamp(<in>, <fmt>) AS result
        """
      Then query result
        | result              |
        | 2026-06-15 00:00:00 |

      Examples:
        | case                                                      | in             | fmt                  |
        | `to_timestamp` treats quoted day name as a literal        | '2026-06-15 E' | "yyyy-MM-dd 'E'" |
        | `to_timestamp` treats quoted aligned day as a literal      | '2026-06-15 F' | "yyyy-MM-dd 'F'" |
        | `to_timestamp` treats quoted stand-alone quarter literally | '2026-06-15 q' | "yyyy-MM-dd 'q'" |
        | `to_timestamp` treats quoted quarter as a literal         | '2026-06-15 Q' | "yyyy-MM-dd 'Q'" |
        | `to_timestamp` treats quoted week-based year literally    | '2026-06-15 Y' | "yyyy-MM-dd 'Y'" |
        | `to_timestamp` treats quoted day-period as a literal       | '2026-06-15 B' | "yyyy-MM-dd 'B'" |
        | `to_timestamp` treats quoted unknown letter as a literal   | '2026-06-15 C' | "yyyy-MM-dd 'C'" |

  Rule: Pattern validation and error handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Invalid pattern: <case>
      When query
        """
        SELECT to_timestamp('<in>', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                              | in               | fmt         |
        | `to_timestamp` rejects unclosed optional section  | 2026-06-15T14:30 | yyyy-MM-dd[ |
        | `to_timestamp` rejects unexpected closing bracket | 2026-06-15T14:30 | yyyy-MM-dd] |
