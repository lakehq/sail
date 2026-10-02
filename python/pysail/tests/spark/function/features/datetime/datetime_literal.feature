# Moved from features/datetime_literal.feature by the datetime/ layout reorganisation.
Feature: Datetime literal syntax from Spark SQL documentation

  This feature tests datetime literal syntax as documented in Spark 4.1.2:
  https://spark.apache.org/docs/4.1.2/sql-ref-literals.html#datetime-literal

  Rule: DATE literal syntax

    Scenario Outline: DATE literal: <case>
      When query
        """
        SELECT DATE '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                         | lit         | col        |
        | DATE literal with year only                  | 1997        | 1997-01-01 |
        | DATE literal with year and month             | 1997-01     | 1997-01-01 |
        | DATE literal with full date                  | 2011-11-11  | 2011-11-11 |
        | DATE literal with leading positive year sign | +1997-01-31 | 1997-01-31 |

    Scenario: DATE literal with year only
      When query
        """
        SELECT DATE '1997' AS col
        """
      Then query result
        | col        |
        | 1997-01-01 |

    Scenario: DATE literal with year and month
      When query
        """
        SELECT DATE '1997-01' AS col
        """
      Then query result
        | col        |
        | 1997-01-01 |

    Scenario: DATE literal with full date
      When query
        """
        SELECT DATE '2011-11-11' AS col
        """
      Then query result
        | col        |
        | 2011-11-11 |

  Rule: TIME literal syntax

    Scenario Outline: TIME literal: <case>
      When query
        """
        SELECT TIME '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                                    | lit             | col             |
        | TIME literal with hour and minute                       | 12:00           | 12:00:00        |
        | TIME literal with single digit hour and minute          | 2:0             | 02:00:00        |
        | TIME literal with single digit hour, minute, and second | 2:0:3           | 02:00:03        |
        | TIME literal with microseconds                          | 23:59:59.999999 | 23:59:59.999999 |

    Scenario: TIME literal with hour and minute
      When query
        """
        SELECT TIME '12:00' AS col
        """
      Then query result
        | col      |
        | 12:00:00 |

    Scenario: TIME literal with single digit hour and minute
      When query
        """
        SELECT TIME '2:0' AS col
        """
      Then query result
        | col      |
        | 02:00:00 |

    Scenario: TIME literal with single digit hour, minute, and second
      When query
        """
        SELECT TIME '2:0:3' AS col
        """
      Then query result
        | col      |
        | 02:00:03 |

    Scenario: TIME literal with microseconds
      When query
        """
        SELECT TIME '23:59:59.999999' AS col
        """
      Then query result
        | col             |
        | 23:59:59.999999 |

  Rule: TIMESTAMP literal syntax

    Scenario Outline: TIMESTAMP literal: <case>
      When query
        """
        SELECT TIMESTAMP '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                              | lit                                   | col                        |
        | TIMESTAMP literal with milliseconds               | 1997-01-31 09:26:56.123               | 1997-01-31 09:26:56.123    |
        | TIMESTAMP literal with year and month only        | 1997-01                               | 1997-01-01 00:00:00        |
        | TIMESTAMP literal with timezone conversion        | 1997-01-31 09:26:56.66666666UTC+08:00 | 1997-01-31 01:26:56.666666 |
        | TIMESTAMP literal with leading positive year sign | +1997-01-31 09:26:56                  | 1997-01-31 09:26:56        |

    Scenario: TIMESTAMP literal with milliseconds
      When query
        """
        SELECT TIMESTAMP '1997-01-31 09:26:56.123' AS col
        """
      Then query result
        | col                     |
        | 1997-01-31 09:26:56.123 |

    Scenario: TIMESTAMP literal with year and month only
      When query
        """
        SELECT TIMESTAMP '1997-01' AS col
        """
      Then query result
        | col                |
        | 1997-01-01 00:00:00 |

    Scenario: TIMESTAMP literal with timezone conversion
      When query
        """
        SELECT TIMESTAMP '1997-01-31 09:26:56.66666666UTC+08:00' AS col
        """
      Then query result
        | col                      |
        | 1997-01-31 01:26:56.666666 |

  Rule: Nanosecond precision handling

    The parser accepts up to 9 digits for nanoseconds, but Spark stores timestamps
    with microsecond precision. Nanosecond part is truncated during conversion.

    Scenario Outline: Nanoseconds: <case>
      When query
        """
        SELECT <type> '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                                                 | type          | lit                           | col                        |
        | TIMESTAMP literal with 9-digit nanoseconds truncates to microseconds | TIMESTAMP     | 2026-06-15 14:30:45.123456789 | 2026-06-15 14:30:45.123456 |
        | TIMESTAMP literal with nanoseconds at maximum value                  | TIMESTAMP     | 2026-06-15 14:30:45.999999999 | 2026-06-15 14:30:45.999999 |
        | TIMESTAMP literal with nanoseconds at minimum value                  | TIMESTAMP     | 2026-06-15 14:30:45.000000001 | 2026-06-15 14:30:45        |
        | TIMESTAMP_NTZ literal with nanosecond truncation                     | TIMESTAMP_NTZ | 2026-06-15 14:30:45.123456789 | 2026-06-15 14:30:45.123456 |
        | TIMESTAMP_LTZ literal with nanosecond truncation                     | TIMESTAMP_LTZ | 2026-06-15 14:30:45.123456789 | 2026-06-15 14:30:45.123456 |

    Scenario: TIMESTAMP literal with 9-digit nanoseconds truncates to microseconds
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.123456789' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: TIMESTAMP literal with nanoseconds at maximum value
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.999999999' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.999999 |

    Scenario: TIMESTAMP literal with nanoseconds at minimum value
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.000000001' AS col
        """
      Then query result
        | col                |
        | 2026-06-15 14:30:45 |

    Scenario: TIMESTAMP_NTZ literal with nanosecond truncation
      When query
        """
        SELECT TIMESTAMP_NTZ '2026-06-15 14:30:45.123456789' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: TIMESTAMP_LTZ literal with nanosecond truncation
      When query
        """
        SELECT TIMESTAMP_LTZ '2026-06-15 14:30:45.123456789' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.123456 |

  Rule: Timezone handling in TIMESTAMP literals

    Scenario Outline: Timezone: <case>
      When query
        """
        SELECT TIMESTAMP '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                       | lit                                         | col                        |
        | TIMESTAMP literal with Z timezone          | 2026-06-15 14:30:45.123456Z                 | 2026-06-15 14:30:45.123456 |
        | TIMESTAMP literal with UTC offset          | 2026-06-15 14:30:45.123456UTC+00:00         | 2026-06-15 14:30:45.123456 |
        | TIMESTAMP literal with negative UTC offset | 2026-06-15 14:30:45.123456UTC-05:00         | 2026-06-15 19:30:45.123456 |
        | TIMESTAMP literal with named timezone      | 2026-06-15 14:30:45.123456 America/New_York | 2026-06-15 18:30:45.123456 |

    Scenario: TIMESTAMP literal with Z timezone
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.123456Z' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: TIMESTAMP literal with UTC offset
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.123456UTC+00:00' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 14:30:45.123456 |

    Scenario: TIMESTAMP literal with negative UTC offset
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.123456UTC-05:00' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 19:30:45.123456 |

    Scenario: TIMESTAMP literal with named timezone
      When query
        """
        SELECT TIMESTAMP '2026-06-15 14:30:45.123456 America/New_York' AS col
        """
      Then query result
        | col                      |
        | 2026-06-15 18:30:45.123456 |

  Rule: Literals at the limits of the 0001-9999 range

    # Spark 4.2.0 AstBuilder.visitTypeConstructor parses with SparkDateTimeUtils.stringToTimestamp /
    # stringToDate: years up to +/-294247 (timestamps) are valid, year 0 and negative years included,
    # and a zone suffix can push the instant past either end of 0001-9999.

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: Range limit: <case>
      When query
        """
        SELECT <type> '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                                           | type          | lit                               | col                          |
        | minimum timestamp shifted into year 0          | TIMESTAMP     | 0001-01-01 00:00:00+14:00         | 0000-12-31 10:00:00          |
        | maximum timestamp shifted into year 10000      | TIMESTAMP     | 9999-12-31 23:59:59.999999-12:00  | +10000-01-01 11:59:59.999999 |
        | timestamp in year 10000                        | TIMESTAMP_NTZ | +10000-01-01 00:00:00             | +10000-01-01 00:00:00        |
        | negative year timestamp                        | TIMESTAMP     | -0001-01-01 00:00:00              | -0001-01-01 00:00:00         |
        | year zero date                                 | DATE          | 0000-01-01                        | 0000-01-01                   |
        | negative year date                             | DATE          | -0001-12-31                       | -0001-12-31                  |
        | offset at the -18:00 limit                     | TIMESTAMP     | 2024-01-01 00:00:00-18:00         | 2024-01-01 18:00:00          |
        | quarter-hour region id                         | TIMESTAMP     | 2024-01-01 00:00:00 Asia/Kathmandu | 2023-12-31 18:15:00          |

    @sail-bug
    Scenario: TIMESTAMP literal beyond Sail's year range
      When query
        """
        SELECT TIMESTAMP '294247-01-01 00:00:00' AS col
        """
      Then query result
        | col                    |
        | +294247-01-01 00:00:00 |

    @sail-bug
    Scenario Outline: TIMESTAMP literal with a short or prefixed zone id: <case>
      When query
        """
        SELECT TIMESTAMP '<lit>' AS col
        """
      Then query result
        | col   |
        | <col> |

      Examples:
        | case                 | lit                           | col                 |
        | short id PST         | 2024-01-01 00:00:00 PST       | 2024-01-01 08:00:00 |
        | GMT-prefixed offset  | 2024-01-01 00:00:00 GMT+08:00 | 2023-12-31 16:00:00 |
        | UTC-prefixed offset  | 2024-01-01 00:00:00 UTC+8     | 2023-12-31 16:00:00 |

  Rule: An invalid datetime literal is INVALID_TYPED_LITERAL

    # Spark 4.2.0 AstBuilder.visitTypeConstructor: when stringToTimestamp / stringToDate returns
    # None the parser throws QueryParsingErrors.cannotParseValueTypeError (INVALID_TYPED_LITERAL).
    # A zone offset beyond +/-18:00 is invalid too, including on TIMESTAMP_NTZ.

    Background:
      Given config spark.sql.session.timeZone = UTC

    @sail-bug
    Scenario Outline: Invalid literal: <case>
      When query
        """
        SELECT <type> '<lit>' AS col
        """
      Then query error INVALID_TYPED_LITERAL

      Examples:
        | case                                 | type          | lit                            |
        | offset beyond +18:00                 | TIMESTAMP     | 2024-01-01 00:00:00+19:00      |
        | offset one minute beyond +18:00      | TIMESTAMP     | 2024-01-01 00:00:00+18:01      |
        | LTZ offset beyond +18:00             | TIMESTAMP_LTZ | 2024-01-01 00:00:00+19:00      |
        | NTZ with an out-of-range offset      | TIMESTAMP_NTZ | 2024-01-01 00:00:00+19:00      |
        | unknown region id                    | TIMESTAMP     | 2024-01-01 00:00:00 Not/AZone  |
        | day 30 of February                   | TIMESTAMP     | 2024-02-30 00:00:00            |
        | month 13                             | TIMESTAMP     | 2024-13-01 00:00:00            |
        | February 29 of a common year         | DATE          | 2023-02-29                     |
        | month 0                              | DATE          | 2024-00-10                     |
        | February 29 of year -1               | DATE          | -0001-02-29                    |

  Rule: Literals in a DST transition are resolved in the session time zone

    Scenario: TIMESTAMP literals inside the spring-forward gap and the fall-back overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          TIMESTAMP '2024-03-10 02:30:00' AS gap,
          TIMESTAMP '2024-11-03 01:30:00' = TIMESTAMP '2024-11-03 01:30:00-07:00' AS overlap_is_earlier,
          TIMESTAMP_NTZ '2024-03-10 02:30:00' AS ntz
        """
      Then query result
        | gap                 | overlap_is_earlier | ntz                 |
        | 2024-03-10 03:30:00 | true               | 2024-03-10 02:30:00 |
