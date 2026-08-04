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
