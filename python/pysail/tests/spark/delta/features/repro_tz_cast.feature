@sail-only
Feature: Relaxed timezone cast reproducibility scenarios

  # These scenarios verify that Sail-native Delta tables do not produce a
  # RelaxedTzCastExec node in execution plans.  The patterns mirror real-world
  # dbt SCD (Slowly Changing Dimension) snapshot SQL.

  Rule: Case A - simple timestamp, nullif, and coalesce(nullif) queries on a single-column Delta table

    Background:
      Given variable location for temporary directory repro_a
      Given final statement
        """
        DROP TABLE IF EXISTS repro_a
        """
      Given statement template
        """
        CREATE TABLE repro_a (ts TIMESTAMP) USING delta LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO repro_a VALUES (TIMESTAMP '1981-05-20 06:46:51')
        """

    Scenario: EXPLAIN SELECT ts shows no RelaxedTzCastExec
      When query
        """
        EXPLAIN SELECT ts FROM repro_a
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN SELECT nullif(ts, ts) shows no RelaxedTzCastExec
      When query
        """
        EXPLAIN SELECT nullif(ts, ts) FROM repro_a
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN SELECT coalesce(nullif(ts, ts), null) shows no RelaxedTzCastExec
      When query
        """
        EXPLAIN SELECT coalesce(nullif(ts, ts), null) FROM repro_a
        """
      Then query plan matches snapshot

  Rule: Case B - multi-column table with the dbt_valid_from / dbt_valid_to pattern

    Background:
      Given variable location for temporary directory repro_b
      Given final statement
        """
        DROP TABLE IF EXISTS repro_b
        """
      Given statement template
        """
        CREATE TABLE repro_b (id INT, name STRING, some_date TIMESTAMP) USING delta LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO repro_b VALUES
          (cast(1 as int),  cast('Easton'   as string), cast('1981-05-20T06:46:51' as timestamp)),
          (cast(2 as int),  cast('Lillian'  as string), cast('1978-09-03T18:10:33' as timestamp)),
          (cast(3 as int),  cast('Jeremiah' as string), cast('1982-03-11T03:59:51' as timestamp)),
          (cast(10 as int), cast('Nora'     as string), cast('1976-03-01T16:51:39' as timestamp))
        """

    Scenario: EXPLAIN SELECT coalesce(nullif(some_date, some_date), null) shows no RelaxedTzCastExec
      When query
        """
        EXPLAIN SELECT coalesce(nullif(some_date, some_date), null) FROM repro_b
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN SELECT with dbt_valid_from and dbt_valid_to pattern shows no RelaxedTzCastExec
      When query
        """
        EXPLAIN SELECT
          some_date AS dbt_valid_from,
          coalesce(nullif(some_date, some_date), null) AS dbt_valid_to
        FROM repro_b
        """
      Then query plan matches snapshot

  Rule: Case C - dbt SCD snapshot table CTAS with nullif pattern

    Background:
      Given variable location_base for temporary directory repro_base_c
      Given variable location_snap for temporary directory repro_snap_c
      Given final statement
        """
        DROP TABLE IF EXISTS repro_snap_c
        """
      Given final statement
        """
        DROP TABLE IF EXISTS repro_base_c
        """
      Given statement template
        """
        CREATE TABLE repro_base_c (id INT, name STRING, some_date TIMESTAMP)
        USING delta LOCATION {{ location_base.sql }}
        """
      Given statement
        """
        INSERT INTO repro_base_c VALUES
          (cast(1 as int),  cast('Easton'  as string), cast('1981-05-20T06:46:51' as timestamp)),
          (cast(2 as int),  cast('Lillian' as string), cast('1978-09-03T18:10:33' as timestamp))
        """

    Scenario: EXPLAIN shows plan for dbt SCD snapshot SELECT with md5 hash and nullif pattern
      When query
        """
        EXPLAIN SELECT *,
          md5(
            coalesce(cast(id as string), '') || '|' ||
            coalesce(cast(some_date as string), '')
          ) AS dbt_scd_id,
          some_date AS dbt_updated_at,
          some_date AS dbt_valid_from,
          coalesce(nullif(some_date, some_date), null) AS dbt_valid_to
        FROM (
          SELECT * FROM repro_base_c
        ) sbq
        """
      Then query plan matches snapshot

  Rule: Case D - full dbt SCD snapshot merge workflow (depends on Case C setup)

    Background:
      Given variable location_base for temporary directory repro_base_d
      Given variable location_snap for temporary directory repro_snap_d
      Given final statement
        """
        DROP TABLE IF EXISTS repro_snap_d
        """
      Given final statement
        """
        DROP TABLE IF EXISTS repro_base_d
        """
      Given statement template
        """
        CREATE TABLE repro_base_d (id INT, name STRING, some_date TIMESTAMP)
        USING delta LOCATION {{ location_base.sql }}
        """
      Given statement
        """
        INSERT INTO repro_base_d VALUES
          (cast(1 as int),  cast('Easton'  as string), cast('1981-05-20T06:46:51' as timestamp)),
          (cast(2 as int),  cast('Lillian' as string), cast('1978-09-03T18:10:33' as timestamp))
        """
      Given statement template
        """
        CREATE OR REPLACE TABLE repro_snap_d
        USING delta LOCATION {{ location_snap.sql }}
        AS SELECT *,
          md5(
            coalesce(cast(id as string), '') || '|' ||
            coalesce(cast(some_date as string), '')
          ) AS dbt_scd_id,
          some_date AS dbt_updated_at,
          some_date AS dbt_valid_from,
          coalesce(nullif(some_date, some_date), null) AS dbt_valid_to
        FROM (
          SELECT * FROM repro_base_d
        ) sbq
        """

    Scenario: EXPLAIN shows plan for reading snapshot table timestamp columns
      When query
        """
        EXPLAIN SELECT dbt_valid_from, dbt_valid_to FROM repro_snap_d
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN shows plan for snapshotted_data CTE reading from snapshot table
      When query
        """
        EXPLAIN
        WITH snapshotted_data AS (
          SELECT *, id AS dbt_unique_key
          FROM repro_snap_d
          WHERE dbt_valid_to IS NULL
        )
        SELECT * FROM snapshotted_data
        """
      Then query plan matches snapshot
