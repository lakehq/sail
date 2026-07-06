@date_trunc
Feature: DATE_TRUNC always returns a timestamp

  Rule: date_trunc returns timestamp for every date-family input type

    Scenario: date_trunc on timestamp column returns timestamp type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP '2026-02-02 00:00:00 UTC'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz column returns timestamp type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz literal returns timestamp type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on date literal returns timestamp type
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-01-15') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on date column returns timestamp type
      When query
      """
      WITH t(d) AS (VALUES (DATE '2026-02-02'))
      SELECT date_trunc('YEAR', d) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc on timestamp_ntz literal value
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP_NTZ '2024-01-15 12:34:56') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on string date value coerces to timestamp
      When query
      """
      SELECT date_trunc('DAY', '2024-01-15') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on string timestamp value coerces to timestamp
      When query
      """
      SELECT date_trunc('DAY', '2024-01-15 12:34:56') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

  Rule: truncation unit selects the granularity

    Scenario: date_trunc YEAR truncates to start of year
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    Scenario: date_trunc QUARTER truncates to start of quarter
      When query
      """
      SELECT date_trunc('QUARTER', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-04-01 00:00:00 |

    Scenario: date_trunc MONTH truncates to start of month
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-01 00:00:00 |

    Scenario: date_trunc WEEK truncates to Monday
      When query
      """
      SELECT date_trunc('WEEK', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-13 00:00:00 |

    Scenario: date_trunc DAY truncates to start of day
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

    Scenario: date_trunc HOUR truncates to start of hour
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:00:00 |

    Scenario: date_trunc MINUTE truncates to start of minute
      When query
      """
      SELECT date_trunc('MINUTE', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:00 |

    Scenario: date_trunc SECOND truncates fractional seconds
      When query
      """
      SELECT date_trunc('SECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:30 |

    Scenario: date_trunc MILLISECOND keeps milliseconds
      When query
      """
      SELECT date_trunc('MILLISECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result                  |
      | 2024-05-15 13:45:30.123 |

    Scenario: date_trunc MICROSECOND keeps microseconds
      When query
      """
      SELECT date_trunc('MICROSECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result                     |
      | 2024-05-15 13:45:30.123456 |

  Rule: unit aliases and case are accepted

    Scenario: date_trunc accepts YYYY alias for year
      When query
      """
      SELECT date_trunc('YYYY', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    Scenario: date_trunc accepts MM alias for month
      When query
      """
      SELECT date_trunc('MM', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-05-01 00:00:00 |

    Scenario: date_trunc accepts DD alias for day
      When query
      """
      SELECT date_trunc('DD', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

    Scenario: date_trunc unit is case insensitive
      When query
      """
      SELECT date_trunc('yEaR', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

  Rule: invalid or null unit returns null

    Scenario: date_trunc with an unrecognized unit returns null
      When query
      """
      SELECT date_trunc('INVALID', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc with an empty unit returns null
      When query
      """
      SELECT date_trunc('', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc with a null unit returns null
      When query
      """
      SELECT date_trunc(CAST(NULL AS STRING), TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: null value returns null

    Scenario: date_trunc on a null date returns null
      When query
      """
      SELECT date_trunc('DAY', CAST(NULL AS DATE)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc on a null timestamp returns null
      When query
      """
      SELECT date_trunc('DAY', CAST(NULL AS TIMESTAMP)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: date_trunc rejects non-date input types

    Scenario: date_trunc rejects a time value
      When query
      """
      SELECT date_trunc('DAY', TIME '12:30:00') AS result
      """
      Then query error .*

    Scenario: date_trunc rejects an integer value
      When query
      """
      SELECT date_trunc('DAY', 1) AS result
      """
      Then query error .*

  Rule: date boundaries and leap days

    @sail-bug
    Scenario: date_trunc YEAR at the maximum supported year
      When query
      """
      SELECT date_trunc('YEAR', DATE '9999-12-31') AS result
      """
      Then query result
      | result              |
      | 9999-01-01 00:00:00 |

    Scenario: date_trunc DAY on a leap day
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-02-29') AS result
      """
      Then query result
      | result              |
      | 2024-02-29 00:00:00 |

  Rule: timezone-aware truncation on timestamps

    Scenario: date_trunc YEAR on timestamp values
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-01 00:00:00 |

    Scenario: date_trunc DAY on timestamp with America/Los_Angeles timezone
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2026-03-15 02:30:00 America/Los_Angeles') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 00:00:00 |

    Scenario: date_trunc HOUR on timestamp with America/New_York timezone
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2026-03-15 14:45:30 America/New_York') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 18:00:00 |

  Rule: truncated date compares equal to a timestamp literal

    Scenario: date_trunc on date equals timestamp literal
      When query
      """
      SELECT count(*) AS result FROM (SELECT DATE '2019-08-04' AS d)
      WHERE date_trunc('year', d) = TIMESTAMP '2019-01-01 00:00:00'
      """
      Then query result
      | result |
      | 1      |
