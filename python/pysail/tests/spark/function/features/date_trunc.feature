@date_trunc
Feature: DATE_TRUNC and TRUNC truncate to a unit
  # `date_trunc` (TruncTimestamp) and `trunc` (TruncDate) are different Spark expressions and do
  # NOT accept the same units: `trunc` only truncates to date-level units, and anything finer --
  # or unrecognized, or NULL -- is NULL, not an error. They are covered together because a single
  # unit table in the plan builder governs both.

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

  Rule: the unit is resolved per row when it comes from a column

    # The unit is not required to be a literal: Spark resolves it row by row, so each row is
    # truncated by its own unit and an unrecognized or NULL unit nullifies only that row.

    Scenario: date_trunc with the unit coming from a column
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), ('MONTH') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | 2026-02-01 00:00:00 |

    Scenario: an unrecognized unit in a column nullifies only its own row
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), ('INVALID') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | NULL                |

    Scenario: a null unit in a column nullifies only its own row
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), (NULL) AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | NULL                |

    Scenario: units in a column keep their aliases and are case insensitive
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('yyyy'), ('MM'), ('Day'), ('hour') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | 2026-02-01 00:00:00 |
      | 2026-02-02 00:00:00 |
      | 2026-02-02 10:00:00 |

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

    Scenario: date_trunc on an untyped null value returns a null timestamp
      When query
      """
      SELECT date_trunc('DAY', NULL) AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

  Rule: a non-string unit is measured as its string form

    Scenario: an integer unit matches no granularity and returns null
      When query
      """
      SELECT date_trunc(1, TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: an unparseable string value under ANSI on errors

    Scenario: an unparseable string value errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc('DAY', 'not a date') AS result
      """
      Then query error .*

    Scenario: an empty string value errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc('DAY', '') AS result
      """
      Then query error .*

  Rule: an unparseable string value under ANSI off returns NULL

    Scenario: an unparseable string value returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', 'not a date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: an empty string value returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', '') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: an unparseable string value nulls only its own row under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', s) AS result FROM VALUES ('2024-05-15'), ('not a date'), (NULL) AS t(s)
      """
      Then query result ordered
      | result              |
      | 2024-05-15 00:00:00 |
      | NULL                |
      | NULL                |

  Rule: date_trunc rejects non-date input types

    # The value is rejected for its TYPE, whatever the unit is: an unrecognized or NULL unit
    # does not turn the rejection into a NULL.

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

    Scenario: date_trunc rejects an integer value even when the unit is unrecognized
      When query
      """
      SELECT date_trunc('INVALID', 1) AS result
      """
      Then query error .*

    Scenario: date_trunc rejects an integer value even when the unit is null
      When query
      """
      SELECT date_trunc(CAST(NULL AS STRING), 1) AS result
      """
      Then query error .*

    Scenario: date_trunc rejects a boolean value even when the unit is unrecognized
      When query
      """
      SELECT date_trunc('INVALID', true) AS result
      """
      Then query error .*

    Scenario: date_trunc rejects a time value even when the unit is null
      When query
      """
      SELECT date_trunc(NULL, TIME '12:30:00') AS result
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

  Rule: trunc truncates to a date-level unit

    Scenario: trunc to the week
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'week') AS result
        """
      Then query result
        | result     |
        | 2019-07-29 |

    Scenario: trunc to the quarter
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'quarter') AS result
        """
      Then query result
        | result     |
        | 2019-07-01 |

    Scenario: trunc matches the unit case-insensitively
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'MoN') AS result
        """
      Then query result
        | result     |
        | 2019-08-01 |

    Scenario: trunc accepts the yy alias
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'yy') AS result
        """
      Then query result
        | result     |
        | 2019-01-01 |

    Scenario: trunc takes the date from a column
      When query
        """
        SELECT trunc(d, 'month') AS result FROM VALUES (1, DATE '2019-08-04'), (2, DATE '2020-02-29') AS t(i, d) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2019-08-01 |
        | 2020-02-01 |

  Rule: a unit finer than a day is NULL, not a truncation

    Scenario: trunc to the day is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'DAY') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: trunc to the hour is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'HOUR') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: an unrecognized or NULL unit is NULL, not an error

    Scenario: trunc with an unrecognized unit is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'bogus') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: trunc with a NULL unit is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: the unit may come from a column

    # The plan builder only matches `Expr::Literal`, so a unit in a column falls through to
    # DataFusion's `date_trunc`, which demands a scalar. Sail errors: Granularity of `date_trunc`
    # must be non-null scalar Utf8.
    Scenario: trunc resolves the unit of each row
      When query
        """
        SELECT trunc(DATE '2019-08-04', c) AS result FROM VALUES (1, 'year'), (2, 'month'), (3, 'DAY'), (4, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2019-01-01 |
        | 2019-08-01 |
        | NULL       |
        | NULL       |

  Rule: the plan resolves the unit

    # The plan is Sail's, so it cannot be compared against Spark. These snapshots exist to make the
    # shape of the plan reviewable: how the unit is matched, and whether a literal unit still
    # collapses to a single call once the units are enumerated in the plan.
    @sail-only
    Scenario: the plan for a literal unit
      When query
        """
        EXPLAIN SELECT date_trunc('YEAR', ts) AS result FROM VALUES (TIMESTAMP '2024-05-15 13:45:30') AS t(ts)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: the plan for a unit coming from a column
      When query
        """
        EXPLAIN SELECT date_trunc(c, ts) AS result FROM VALUES (TIMESTAMP '2024-05-15 13:45:30', 'YEAR') AS t(ts, c)
        """
      Then query plan matches snapshot

  Rule: date_trunc — the argument may come from a column

    @column_args
    Scenario: date_trunc with the argument as a literal
      When query
        """
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359') AS result
        """
      Then query result ordered
        | result              |
        | 2015-03-01 00:00:00 |

    @column_args
    Scenario: date_trunc takes argument 1 from a column holding two different values
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-03-01 00:00:00 |

    @column_args
    Scenario: date_trunc takes argument 1 from a column
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'YEAR') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-01-01 00:00:00 |
