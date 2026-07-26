# Moved from features/datetime_literal.feature by the datetime/ layout reorganisation.
@datetime_literal
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
        | case                             | lit        | col        |
        | DATE literal with year only      | 1997       | 1997-01-01 |
        | DATE literal with year and month | 1997-01    | 1997-01-01 |
        | DATE literal with full date      | 2011-11-11 | 2011-11-11 |

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
        | case                                       | lit                                   | col                        |
        | TIMESTAMP literal with milliseconds        | 1997-01-31 09:26:56.123               | 1997-01-31 09:26:56.123    |
        | TIMESTAMP literal with year and month only | 1997-01                               | 1997-01-01 00:00:00        |
        | TIMESTAMP literal with timezone conversion | 1997-01-31 09:26:56.66666666UTC+08:00 | 1997-01-31 01:26:56.666666 |

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
