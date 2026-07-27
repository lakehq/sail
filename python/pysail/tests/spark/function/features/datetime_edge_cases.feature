Feature: datetime edge cases

  Rule: 2-digit year expansion boundaries

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_date` expands 2-digit year 00 to 2000
      When query
        """
        SELECT to_date('00-01-01', 'yy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2000-01-01 |

    Scenario: `to_date` expands 2-digit year 49 to 2049
      When query
        """
        SELECT to_date('49-12-31', 'yy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2049-12-31 |

    Scenario: `to_date` expands 2-digit year 50 to 2050
      When query
        """
        SELECT to_date('50-01-01', 'yy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2050-01-01 |

    Scenario: `to_date` expands 2-digit year 99 to 2099
      When query
        """
        SELECT to_date('99-06-15', 'yy-MM-dd') AS result
        """
      Then query result
        | result     |
        | 2099-06-15 |

  Rule: Extreme timezone offsets

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` handles maximum positive offset +14:00
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45+14:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:30:45Z      |

    Scenario: `date_format` handles maximum negative offset -12:00
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45-12:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-16 02:30:45Z      |

    Scenario: `to_timestamp` parses maximum positive offset +14:00
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45+14:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:30:45        |

    Scenario: `to_timestamp` parses maximum negative offset -12:00
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45-12:00', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-16 02:30:45        |

  Rule: Half-hour and quarter-hour timezone offsets

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` handles India timezone +05:30
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45+05:30', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 09:00:45Z      |

    Scenario: `to_timestamp` parses Newfoundland timezone -03:30
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45-03:30', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 18:00:45        |

    Scenario: `to_timestamp` parses Nepal timezone +05:45
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45+05:45', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 08:45:45        |

    Scenario: `to_timestamp` parses Chatham Islands timezone +12:45
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45+12:45', 'yyyy-MM-dd HH:mm:ssXXX') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 01:45:45        |

  Rule: Clock hour edge cases

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` parses 24:00:00 as midnight next day
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result                      |
        | 2026-06-16 00:00:00        |

    Scenario: `to_timestamp` parses k=24 clock hour format
      When query
        """
        SELECT to_timestamp('2026-06-15 24:00:00', 'yyyy-MM-dd kk:mm:ss') AS result
        """
      Then query result
        | result                      |
        | 2026-06-16 00:00:00        |

    Scenario: `to_timestamp` parses 12-hour midnight with AM
      When query
        """
        SELECT to_timestamp('2026-06-15 12:00:00 AM', 'yyyy-MM-dd hh:mm:ss a') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 00:00:00        |

    Scenario: `to_timestamp` parses 12-hour noon with PM
      When query
        """
        SELECT to_timestamp('2026-06-15 12:00:00 PM', 'yyyy-MM-dd hh:mm:ss a') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 12:00:00        |

  Rule: Fractional seconds precision

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` pads fractional seconds to requested width
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123', 'SSSSSSSSS') AS result
        """
      Then query result
        | result     |
        | 123000000 |

    Scenario: `to_timestamp` truncates nanoseconds to microseconds
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                   |
        | 2026-06-15 14:30:45.123456 |

    Scenario: `date_format` formats minimum nanosecond value
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.000000001', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.000000000 |

    Scenario: `date_format` formats maximum nanosecond value
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45.999999000 |

  Rule: Leap second handling

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `to_timestamp` handles leap second 23:59:60
      When query
        """
        SELECT to_timestamp('2026-06-15 23:59:60', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result                      |
        | 2026-06-16 00:00:00        |

    Scenario: `to_timestamp` rejects invalid leap second at wrong time
      When query
        """
        SELECT to_timestamp('2026-06-15 12:30:60', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Era handling with BC dates

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats AD era
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', 'G yyyy-MM-dd') AS result
        """
      Then query result
        | result            |
        | AD 2026-06-15    |

    Scenario: `to_timestamp` parses AD era
      When query
        """
        SELECT to_timestamp('AD 2026-06-15 14:30:45', 'G yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result                      |
        | 2026-06-15 14:30:45        |

  Rule: Week-based fields

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` formats week-of-month
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'W') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: `date_format` formats aligned week-of-month
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'F') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: `date_format` formats quarter
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'Q') AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: `date_format` formats quarter with text
      When query
        """
        SELECT date_format(DATE '2026-06-15', 'QQQ') AS result
        """
      Then query result
        | result |
        | Q2     |

  Rule: Optional sections

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: `date_format` omits optional section when zero
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `date_format` includes optional section when non-zero
      When query
        """
        SELECT date_format(TIMESTAMP '2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result                   |
        | 2026-06-15 14:30:45.123456 |

    Scenario: `to_timestamp` parses without optional section
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result              |
        | 2026-06-15 14:30:45 |

    Scenario: `to_timestamp` parses with optional section
      When query
        """
        SELECT to_timestamp('2026-06-15 14:30:45.123456', 'yyyy-MM-dd HH:mm:ss[.SSSSSS]') AS result
        """
      Then query result
        | result                   |
        | 2026-06-15 14:30:45.123456 |
