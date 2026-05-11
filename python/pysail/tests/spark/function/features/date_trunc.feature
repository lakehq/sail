Feature: DATE_TRUNC preserves timestamp type

  Rule: date_trunc on timestamp preserves type

    Scenario: date_trunc on timestamp column preserves timestamp type
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

    @sail-bug
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

    @sail-bug
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

    @sail-only
    Scenario: date_trunc on timestamp_ntz column preserves timestamp_ntz type in Sail
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = true)
      """

    @sail-only
    Scenario: date_trunc on timestamp_ntz literal preserves timestamp_ntz type in Sail
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = false)
      """

    @sail-bug
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

    @sail-bug
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

    @sail-bug
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

    @sail-bug
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

  Rule: Preimage — plan snapshots (validates filter rewrite fires)

    @sail-only
    Scenario: EXPLAIN WHERE date_trunc YEAR rewrites to year range
      When query
        """
        EXPLAIN SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-06-15 10:30:00')
          AS t(ts)
        WHERE date_trunc('YEAR', ts) = TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE date_trunc MONTH rewrites to month range
      When query
        """
        EXPLAIN SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-03-15 10:30:00')
          AS t(ts)
        WHERE date_trunc('MONTH', ts) = TIMESTAMP_NTZ '2024-03-01 00:00:00'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN WHERE date_trunc YEAR on zoned timestamp does NOT rewrite
      When query
        """
        EXPLAIN SELECT ts FROM VALUES
          (TIMESTAMP '2024-06-15 10:30:00 UTC')
          AS t(ts)
        WHERE date_trunc('YEAR', ts) = TIMESTAMP '2024-01-01 00:00:00 UTC'
        """
      Then query plan matches snapshot

  Rule: Plan snapshot — filter pushdown on Parquet (preimage)

    @sail-only
    Scenario: EXPLAIN literal timestamp filter on Parquet — baseline
      Given variable location for temporary directory explain_date_trunc_baseline
      Given final statement
        """
        DROP TABLE IF EXISTS explain_date_trunc_baseline_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_date_trunc_baseline_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP_NTZ '2023-06-15 10:00:00'),
          (TIMESTAMP_NTZ '2024-01-01 00:00:00'),
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (TIMESTAMP_NTZ '2024-12-31 23:59:59'),
          (TIMESTAMP_NTZ '2025-03-01 08:00:00')
        AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM explain_date_trunc_baseline_parquet
        WHERE ts >= TIMESTAMP_NTZ '2024-01-01 00:00:00' AND ts < TIMESTAMP_NTZ '2025-01-01 00:00:00'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN date_trunc YEAR filter on Parquet shows preimage pushdown
      Given variable location for temporary directory explain_date_trunc_year
      Given final statement
        """
        DROP TABLE IF EXISTS explain_date_trunc_year_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_date_trunc_year_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP_NTZ '2023-06-15 10:00:00'),
          (TIMESTAMP_NTZ '2024-01-01 00:00:00'),
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (TIMESTAMP_NTZ '2024-12-31 23:59:59'),
          (TIMESTAMP_NTZ '2025-03-01 08:00:00')
        AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM explain_date_trunc_year_parquet
        WHERE date_trunc('YEAR', ts) = TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN date_trunc MONTH filter on Parquet shows preimage pushdown
      Given variable location for temporary directory explain_date_trunc_month
      Given final statement
        """
        DROP TABLE IF EXISTS explain_date_trunc_month_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_date_trunc_month_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP_NTZ '2024-02-15 10:00:00'),
          (TIMESTAMP_NTZ '2024-03-01 00:00:00'),
          (TIMESTAMP_NTZ '2024-03-15 10:30:00'),
          (TIMESTAMP_NTZ '2024-04-01 00:00:00'),
          (TIMESTAMP_NTZ '2024-05-10 08:00:00')
        AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM explain_date_trunc_month_parquet
        WHERE date_trunc('MONTH', ts) = TIMESTAMP_NTZ '2024-03-01 00:00:00'
        """
      Then query plan matches snapshot

  Rule: Spark unit aliases are normalized correctly

    Scenario: date_trunc yy alias truncates to year
      When query
        """
        SELECT date_trunc('yy', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: date_trunc yyyy alias truncates to year
      When query
        """
        SELECT date_trunc('yyyy', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: date_trunc mm alias truncates to month
      When query
        """
        SELECT date_trunc('mm', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-05-01 00:00:00 |

    Scenario: date_trunc mon alias truncates to month
      When query
        """
        SELECT date_trunc('mon', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-05-01 00:00:00 |

    Scenario: date_trunc dd alias truncates to day
      When query
        """
        SELECT date_trunc('dd', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-05-15 00:00:00 |

    Scenario: date_trunc aliases are case-insensitive
      When query
        """
        SELECT date_trunc('YY', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

  Rule: quarter and week units are supported

    Scenario: date_trunc quarter truncates to start of quarter Q1
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2026-02-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:00 |

    Scenario: date_trunc quarter truncates to start of quarter Q3
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2026-07-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 2026-07-01 00:00:00 |

    Scenario: date_trunc week truncates to start of week Monday
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result              |
        | 2026-05-11 00:00:00 |

    Scenario: date_trunc week on the Monday itself is unchanged
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2026-05-11 00:00:00') AS result
        """
      Then query result
        | result              |
        | 2026-05-11 00:00:00 |

  Rule: millisecond and microsecond units are supported

    Scenario: date_trunc millisecond preserves milliseconds and zeroes microseconds
      When query
        """
        SELECT date_trunc('millisecond', TIMESTAMP_NTZ '2026-05-15 10:30:45.123456') AS result
        """
      Then query result
        | result                   |
        | 2026-05-15 10:30:45.123  |

    Scenario: date_trunc microsecond is identity for microsecond precision
      When query
        """
        SELECT date_trunc('microsecond', TIMESTAMP_NTZ '2026-05-15 10:30:45.123456') AS result
        """
      Then query result
        | result                      |
        | 2026-05-15 10:30:45.123456 |

  Rule: unknown unit returns NULL

    @sail-bug
    Scenario: date_trunc with invalid unit returns NULL
      When query
        """
        SELECT date_trunc('INVALID_UNIT', TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: date_trunc with NULL unit returns NULL
      When query
        """
        SELECT date_trunc(NULL, TIMESTAMP_NTZ '2026-05-15 10:30:00') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: DATE input is coerced to TIMESTAMP

    Scenario: date_trunc accepts DATE input and returns correct value
      When query
        """
        SELECT date_trunc('MONTH', DATE '2026-05-15') AS result
        """
      Then query result
        | result              |
        | 2026-05-01 00:00:00 |

    @sail-bug
    Scenario: date_trunc on DATE input returns timestamp schema
      When query
        """
        SELECT date_trunc('MONTH', DATE '2026-05-15') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Preimage — NULL rows in the filtered column are excluded

    Scenario: date_trunc YEAR filter excludes NULL ts rows
      When query
        """
        SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (CAST(NULL AS TIMESTAMP_NTZ)),
          (TIMESTAMP_NTZ '2023-06-15 10:30:00')
        AS t(ts)
        WHERE date_trunc('YEAR', ts) = TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query result
        | ts                  |
        | 2024-06-15 10:30:00 |

    Scenario: date_trunc YEAR != filter excludes NULL ts rows
      When query
        """
        SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (CAST(NULL AS TIMESTAMP_NTZ)),
          (TIMESTAMP_NTZ '2023-06-15 10:30:00')
        AS t(ts)
        WHERE date_trunc('YEAR', ts) != TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query result
        | ts                  |
        | 2023-06-15 10:30:00 |

  Rule: Plan snapshot — filter pushdown on Parquet (preimage)

    @sail-only
    Scenario: EXPLAIN date_trunc with non-boundary literal does NOT rewrite
      Given variable location for temporary directory explain_date_trunc_unsat
      Given final statement
        """
        DROP TABLE IF EXISTS explain_date_trunc_unsat_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_date_trunc_unsat_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP_NTZ '2024-03-15 10:00:00'),
          (TIMESTAMP_NTZ '2024-03-20 12:00:00')
        AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM explain_date_trunc_unsat_parquet
        WHERE date_trunc('MONTH', ts) = TIMESTAMP_NTZ '2024-03-15 10:00:00'
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN date_trunc YEAR != boundary rewrites to disjunction on Parquet
      Given variable location for temporary directory explain_date_trunc_neq_year
      Given final statement
        """
        DROP TABLE IF EXISTS explain_date_trunc_neq_year_parquet
        """
      Given statement template
        """
        CREATE TABLE explain_date_trunc_neq_year_parquet
        USING PARQUET
        LOCATION {{ location.sql }}
        AS SELECT * FROM VALUES
          (TIMESTAMP_NTZ '2023-06-15 10:00:00'),
          (TIMESTAMP_NTZ '2024-03-01 00:00:00'),
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (TIMESTAMP_NTZ '2025-03-01 08:00:00')
        AS t(ts)
        """
      When query
        """
        EXPLAIN SELECT ts FROM explain_date_trunc_neq_year_parquet
        WHERE date_trunc('YEAR', ts) != TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query plan matches snapshot

  Rule: Preimage != plan snapshots (validates disjunction rewrite fires)

    @sail-only
    Scenario: EXPLAIN WHERE date_trunc YEAR != boundary rewrites to disjunction
      When query
        """
        EXPLAIN SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-06-15 10:30:00'),
          (TIMESTAMP_NTZ '2023-12-31 23:59:59'),
          (TIMESTAMP_NTZ '2025-01-15 08:00:00')
        AS t(ts)
        WHERE date_trunc('YEAR', ts) != TIMESTAMP_NTZ '2024-01-01 00:00:00'
        """
      Then query plan matches snapshot

  Rule: week and quarter have no preimage (no filter rewrite)

    @sail-only
    Scenario: EXPLAIN WHERE date_trunc WEEK does NOT rewrite
      When query
        """
        EXPLAIN SELECT ts FROM VALUES
          (TIMESTAMP_NTZ '2024-03-18 10:30:00')
        AS t(ts)
        WHERE date_trunc('week', ts) = TIMESTAMP_NTZ '2024-03-18 00:00:00'
        """
      Then query plan matches snapshot

  Rule: Pre-epoch timestamps are handled correctly

    Scenario: date_trunc DAY on pre-epoch timestamp truncates to midnight
      When query
        """
        SELECT date_trunc('DAY', TIMESTAMP_NTZ '1969-12-31 14:30:00') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 00:00:00 |

    Scenario: date_trunc DAY filter on pre-epoch boundary keeps correct rows
      When query
        """
        SELECT count(*) AS c FROM VALUES
          (TIMESTAMP_NTZ '1969-12-31 08:00:00'),
          (TIMESTAMP_NTZ '1969-12-31 23:59:59'),
          (TIMESTAMP_NTZ '1970-01-01 00:00:00')
        AS t(ts)
        WHERE date_trunc('DAY', ts) = TIMESTAMP_NTZ '1969-12-31 00:00:00'
        """
      Then query result
        | c |
        | 2 |

  Rule: Year 9999 boundary does not overflow

    Scenario: date_trunc YEAR on year 9999 does not crash
      When query
        """
        SELECT date_trunc('YEAR', TIMESTAMP_NTZ '9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-01-01 00:00:00 |

    Scenario: date_trunc MONTH on year 9999
      When query
        """
        SELECT date_trunc('MONTH', TIMESTAMP_NTZ '9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-01 00:00:00 |

    Scenario: date_trunc QUARTER on year 9999
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-10-01 00:00:00 |

    Scenario: date_trunc WEEK on year 9999
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-27 00:00:00 |

    Scenario: date_trunc MILLISECOND on year 9999
      When query
        """
        SELECT date_trunc('millisecond', TIMESTAMP_NTZ '9999-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result                  |
        | 9999-12-31 23:59:59.999 |

  Rule: Pre-epoch — all granularities on 1969-12-31 23:59:59.999999

    Scenario: date_trunc SECOND pre-epoch
      When query
        """
        SELECT date_trunc('second', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:59:59 |

    Scenario: date_trunc MINUTE pre-epoch
      When query
        """
        SELECT date_trunc('minute', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:59:00 |

    Scenario: date_trunc HOUR pre-epoch
      When query
        """
        SELECT date_trunc('hour', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:00:00 |

    Scenario: date_trunc WEEK pre-epoch truncates to Monday 1969-12-29
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-12-29 00:00:00 |

    Scenario: date_trunc MONTH pre-epoch
      When query
        """
        SELECT date_trunc('month', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-12-01 00:00:00 |

    Scenario: date_trunc QUARTER pre-epoch Q4
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-10-01 00:00:00 |

    Scenario: date_trunc YEAR pre-epoch
      When query
        """
        SELECT date_trunc('year', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result              |
        | 1969-01-01 00:00:00 |

    Scenario: date_trunc MILLISECOND pre-epoch
      When query
        """
        SELECT date_trunc('millisecond', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result                  |
        | 1969-12-31 23:59:59.999 |

    Scenario: date_trunc MICROSECOND pre-epoch is identity
      When query
        """
        SELECT date_trunc('microsecond', TIMESTAMP_NTZ '1969-12-31 23:59:59.999999') AS result
        """
      Then query result
        | result                      |
        | 1969-12-31 23:59:59.999999 |

    Scenario: date_trunc SECOND pre-epoch with sub-second fractional
      When query
        """
        SELECT date_trunc('second', TIMESTAMP_NTZ '1969-12-31 23:59:59.500000') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:59:59 |

    Scenario: date_trunc MINUTE pre-epoch mid-minute
      When query
        """
        SELECT date_trunc('minute', TIMESTAMP_NTZ '1969-12-31 23:59:30') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:59:00 |

    Scenario: date_trunc HOUR pre-epoch mid-hour
      When query
        """
        SELECT date_trunc('hour', TIMESTAMP_NTZ '1969-12-31 23:30:00') AS result
        """
      Then query result
        | result              |
        | 1969-12-31 23:00:00 |

  Rule: Leap year — Feb 29 truncates correctly

    Scenario: date_trunc DAY on Feb 29 leap year
      When query
        """
        SELECT date_trunc('day', TIMESTAMP_NTZ '2024-02-29 15:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 00:00:00 |

    Scenario: date_trunc WEEK on Feb 29 truncates to Monday Feb 26
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2024-02-29 15:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-26 00:00:00 |

    Scenario: date_trunc MONTH on Feb 29 truncates to Feb 1
      When query
        """
        SELECT date_trunc('month', TIMESTAMP_NTZ '2024-02-29 15:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-01 00:00:00 |

    Scenario: date_trunc QUARTER on Feb 29 truncates to Jan 1
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2024-02-29 15:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

    Scenario: date_trunc YEAR on Feb 29 truncates to Jan 1
      When query
        """
        SELECT date_trunc('year', TIMESTAMP_NTZ '2024-02-29 15:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

  Rule: WEEK crosses year boundary correctly

    Scenario: date_trunc WEEK on Wednesday Jan 1 2025 truncates to Monday Dec 30 2024
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2025-01-01 10:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-12-30 00:00:00 |

    Scenario: date_trunc WEEK on Sunday Dec 29 2024 truncates to Monday Dec 23 2024
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2024-12-29 10:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-12-23 00:00:00 |

    Scenario: date_trunc WEEK on Sunday Jan 5 2025 truncates to Monday Dec 30 2024
      When query
        """
        SELECT date_trunc('week', TIMESTAMP_NTZ '2025-01-05 10:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-12-30 00:00:00 |

  Rule: QUARTER — all four quarter boundaries

    Scenario: date_trunc QUARTER on Apr 1 is Q2 start
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2024-04-01 00:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-04-01 00:00:00 |

    Scenario: date_trunc QUARTER on Jun 30 is still Q2
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2024-06-30 23:59:59') AS result
        """
      Then query result
        | result              |
        | 2024-04-01 00:00:00 |

    Scenario: date_trunc QUARTER on Oct 1 is Q4 start
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2024-10-01 00:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-10-01 00:00:00 |

    Scenario: date_trunc QUARTER on Dec 31 is still Q4
      When query
        """
        SELECT date_trunc('quarter', TIMESTAMP_NTZ '2024-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 2024-10-01 00:00:00 |

  Rule: NULL timestamp input returns NULL

    Scenario: date_trunc with NULL timestamp returns NULL
      When query
        """
        SELECT date_trunc('YEAR', CAST(NULL AS TIMESTAMP_NTZ)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: nanosecond is not a valid Spark unit

    @sail-bug
    Scenario: date_trunc with nanosecond unit returns NULL
      When query
        """
        SELECT date_trunc('nanosecond', TIMESTAMP_NTZ '2024-01-01 00:00:00') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Millisecond sub-precision is zeroed

    Scenario: date_trunc MILLISECOND zeros sub-millisecond micros
      When query
        """
        SELECT date_trunc('millisecond', TIMESTAMP_NTZ '2024-05-15 10:30:45.000001') AS result
        """
      Then query result
        | result              |
        | 2024-05-15 10:30:45 |

    Scenario: date_trunc MILLISECOND on exact millisecond boundary is unchanged
      When query
        """
        SELECT date_trunc('millisecond', TIMESTAMP_NTZ '2024-05-15 10:30:45.001000') AS result
        """
      Then query result
        | result                  |
        | 2024-05-15 10:30:45.001 |

    Scenario: date_trunc SECOND zeros all sub-second precision
      When query
        """
        SELECT date_trunc('second', TIMESTAMP_NTZ '2024-05-15 10:30:45.999999') AS result
        """
      Then query result
        | result              |
        | 2024-05-15 10:30:45 |
