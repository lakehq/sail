Feature: Datetime literal syntax from Spark SQL documentation

  This feature tests datetime literal syntax as documented in Spark 4.1.2:
  https://spark.apache.org/docs/4.1.2/sql-ref-literals.html#datetime-literal

  Rule: DATE literal syntax

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
