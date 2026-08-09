# Moved from features/datetime_format.feature by the datetime/ layout reorganisation.
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
        | `date_format` formats ISO 8601 timestamp with literal separator | 2026-06-01 10:30:45        | "yyyy-MM-dd'T'HH:mm:ss"                   | 2026-06-01T10:30:45  |
        | `date_format` formats standalone fractional seconds             | 2026-06-15 14:30:45.123456 | 'SSSSSS'                                  | 123456               |
        | `date_format` formats text fields                               | 2026-06-15 14:30:45        | 'EEEE, dd MMMM yyyy'                      | Monday, 15 June 2026 |
        | `date_format` formats a dynamic format expression               | 2026-06-15 14:30:45        | concat('yyyy-MM-dd', "'T'", 'HH:mm:ss')   | 2026-06-15T14:30:45  |

  Rule: Spark datetime pattern validation for formatting

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Predefined formatter name: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '2018-11-17 13:33:33.333', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                                       | fmt                  |
        | `date_format` rejects BASIC_ISO_DATE as a predefined name  | BASIC_ISO_DATE       |
        | `date_format` rejects ISO_LOCAL_DATE as a predefined name  | ISO_LOCAL_DATE       |
        | `date_format` rejects ISO_WEEK_DATE as a predefined name   | ISO_WEEK_DATE        |
        | `date_format` rejects RFC_1123_DATE_TIME as a predefined name | RFC_1123_DATE_TIME |

    Scenario Outline: Disabled week-based pattern: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '2018-11-17 13:33:33.333', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                             | fmt |
        | `date_format` rejects week-based year            | Y   |
        | `date_format` rejects week of month              | W   |
        | `date_format` rejects week of year               | w   |
        | `date_format` rejects ISO day number             | u   |
        | `date_format` rejects localized day number       | e   |
        | `date_format` rejects stand-alone day number     | c   |

    Scenario Outline: Invalid Java datetime pattern: <case>
      When query
        """
        SELECT date_format(TIMESTAMP '2018-11-17 13:33:33.333', '<fmt>')
        """
      Then query error .*

      Examples:
        | case                                                        | fmt        |
        | `date_format` rejects narrow era                            | GGGGG      |
        | `date_format` rejects narrow month                          | MMMMM      |
        | `date_format` rejects narrow stand-alone month              | LLLLL      |
        | `date_format` rejects narrow day name                       | EEEEE      |
        | `date_format` rejects narrow quarter                        | QQQQQ      |
        | `date_format` rejects narrow stand-alone quarter            | qqqqq      |
        | `date_format` rejects year wider than six digits            | yyyyyyy    |
        | `date_format` rejects repeated aligned day of week in month | FF         |
        | `date_format` rejects three day-of-month letters            | ddd        |
        | `date_format` rejects four day-of-year letters              | DDDD       |
        | `date_format` rejects three 24-hour letters                 | HHH        |
        | `date_format` rejects three 12-hour letters                 | hhh        |
        | `date_format` rejects three clock-hour letters              | kkk        |
        | `date_format` rejects three am-pm hour letters              | KKK        |
        | `date_format` rejects three minute letters                  | mmm        |
        | `date_format` rejects three second letters                  | sss        |
        | `date_format` rejects ten fractional-second letters         | SSSSSSSSSS |
        | `date_format` rejects repeated am-pm marker                 | aa         |
        | `date_format` rejects single zone ID letter                 | V          |
        | `date_format` rejects five zone-name letters                | zzzzz      |
        | `date_format` rejects six ISO offset letters                | XXXXXX     |
        | `date_format` rejects six localized offset letters          | ZZZZZZ     |
        | `date_format` rejects two localized-zone offset letters     | OO         |
        | `date_format` rejects six lower-case offset letters         | xxxxxx     |
        | `date_format` rejects millisecond-of-day                    | A          |
        | `date_format` rejects day-period                            | B          |
        | `date_format` rejects nano-of-second                        | n          |
        | `date_format` rejects nano-of-day                           | N          |
        | `date_format` rejects pad-next                              | p          |
        | `date_format` rejects unknown pattern letter C              | C          |
        | `date_format` rejects unknown pattern letter I              | I          |

    Scenario: `date_format` treats quoted restricted letters as literals
      When query
        """
        SELECT date_format(DATE '2026-06-15', "yyyy-MM-dd 'Y' 'B' 'E' 'Q'") AS result
        """
      Then query result
        | result             |
        | 2026-06-15 Y B E Q |

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
        | `date_format` formats with day of year               | DATE '2026-06-15'                         | 'yyyy-DDD'                      | 2026-166                      |

  Rule: Java offset pattern formatting semantics

    Scenario: Offset pattern semantics formats UTC with X and x widths 1 through 5
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'X') AS upper_1,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'XX') AS upper_2,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'XXX') AS upper_3,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'XXXX') AS upper_4,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'XXXXX') AS upper_5,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'x') AS lower_1,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'xx') AS lower_2,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'xxx') AS lower_3,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'xxxx') AS lower_4,
          date_format(TIMESTAMP '2026-06-15 14:30:45', 'xxxxx') AS lower_5
        """
      Then query result
        | upper_1 | upper_2 | upper_3 | upper_4 | upper_5 | lower_1 | lower_2 | lower_3 | lower_4 | lower_5 |
        | Z       | Z       | Z       | Z       | Z       | +00     | +0000   | +00:00  | +0000   | +00:00  |

    Scenario: Offset pattern semantics formats a historical second-precision offset
      Given config spark.sql.session.timeZone = Europe/Paris
      When query
        """
        SELECT
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'X') AS upper_1,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'XX') AS upper_2,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'XXX') AS upper_3,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'XXXX') AS upper_4,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'XXXXX') AS upper_5,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'x') AS lower_1,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'xx') AS lower_2,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'xxx') AS lower_3,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'xxxx') AS lower_4,
          date_format(TIMESTAMP '1900-01-01 12:00:00', 'xxxxx') AS lower_5
        """
      Then query result
        | upper_1 | upper_2 | upper_3 | upper_4 | upper_5  | lower_1 | lower_2 | lower_3 | lower_4 | lower_5  |
        | +0009   | +0009   | +00:09  | +000921 | +00:09:21 | +0009   | +0009   | +00:09  | +000921 | +00:09:21 |

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
        | `date_format` formats era with width 4 (GGGG)  | GGGG  | Anno Domini |

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

    # Spark reports arity through WRONG_NUM_ARGS; Sail emits its own wording. Systemic across
    # the whole function surface, so it is recorded rather than worked around here.
    @sail-bug
    Scenario: `date_format` rejects extra locale argument
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'EEEE, dd MMMM yyyy a QQQQ GGGG', 'extra')
        """
      Then query error \[WRONG_NUM_ARGS.*The `date_format` requires 2 parameters but the actual number is 3

    @sail-bug
    Scenario: `from_unixtime` rejects extra locale argument
      When query
        """
        SELECT from_unixtime(1781533845, 'EEEE, dd MMMM yyyy a QQQQ GGGG', 'extra')
        """
      Then query error \[WRONG_NUM_ARGS.*The `from_unixtime` requires \[1, 2\] parameters but the actual number is 3

    @sail-bug
    Scenario Outline: Extra argument: <case>
      When query
        """
        SELECT <fn>('2026-06-15', 'yyyy-MM-dd', 'extra')
        """
      Then query error \[WRONG_NUM_ARGS.*The `<fn>` requires \[1, 2\] parameters but the actual number is 3

      Examples:
        | case                                      | fn               |
        | `to_timestamp` rejects extra argument     | to_timestamp     |
        | `to_timestamp_ltz` rejects extra argument | to_timestamp_ltz |
        | `to_timestamp_ntz` rejects extra argument | to_timestamp_ntz |
        | `try_to_timestamp` rejects extra argument | try_to_timestamp |
