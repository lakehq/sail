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

    Scenario: date_trunc on timestamp_ntz column preserves timestamp_ntz type
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

    Scenario: date_trunc on timestamp_ntz literal preserves timestamp_ntz type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = false)
      """

    Scenario: date_trunc YEAR on timestamp values
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = false)
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
       |-- result: timestamp (nullable = false)
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
       |-- result: timestamp (nullable = false)
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
       |-- result: timestamp (nullable = false)
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
