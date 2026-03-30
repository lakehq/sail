Feature: Delta Lake File Layout

  Rule: Partitioned writes create expected directory structure
    Background:
      Given variable location for temporary directory delta_file_layout
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_partitioned_year
        """
      Given statement template
        """
        CREATE TABLE delta_layout_partitioned_year (
          id INT,
          event STRING,
          year INT,
          score DOUBLE
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: Partitioned by a single column (year) writes into year=... directories
      Given statement
        """
        INSERT INTO delta_layout_partitioned_year VALUES
          (1, 'A', 2025, 0.8),
          (2, 'B', 2025, 0.9),
          (3, 'A', 2026, 0.7),
          (4, 'B', 2026, 0.6)
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_partitioned_year
        """
      Then query result
        | count |
        | 4     |
      Then file tree in location matches
        """
        📂 year=2025
          📄 part-<id>.<codec>.parquet
        📂 year=2026
          📄 part-<id>.<codec>.parquet
        """

  Rule: Multi-column partitioning creates nested directories
    Background:
      Given variable location for temporary directory delta_file_layout_multi
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_partitioned_multi
        """
      Given statement template
        """
        CREATE TABLE delta_layout_partitioned_multi (
          id INT,
          region INT,
          category INT,
          value INT
        )
        USING DELTA
        PARTITIONED BY (region, category)
        LOCATION {{ location.sql }}
        """

    Scenario: Partitioned by (region, category) writes into region=.../category=... directories
      Given statement
        """
        INSERT INTO delta_layout_partitioned_multi VALUES
          (1, 1, 1, 100),
          (2, 1, 2, 200),
          (3, 2, 1, 300),
          (4, 2, 2, 400)
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_partitioned_multi
        """
      Then query result
        | count |
        | 4     |
      Then file tree in location matches
        """
        📂 region=1
          📂 category=1
            📄 part-<id>.<codec>.parquet
          📂 category=2
            📄 part-<id>.<codec>.parquet
        📂 region=2
          📂 category=1
            📄 part-<id>.<codec>.parquet
          📂 category=2
            📄 part-<id>.<codec>.parquet
        """

  Rule: Append reuses existing partition metadata
    Background:
      Given variable location for temporary directory delta_file_layout_append
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_append_partitioned
        """
      Given statement template
        """
        CREATE TABLE delta_layout_append_partitioned (
          id INT,
          category STRING,
          value INT
        )
        USING DELTA
        PARTITIONED BY (category)
        LOCATION {{ location.sql }}
        """

    Scenario: Append adds new data files but keeps existing ones
      Given statement
        """
        INSERT INTO delta_layout_append_partitioned VALUES
          (1, 'A', 10),
          (2, 'B', 20)
        """
      Given remember data files in location as files_v0
      Given statement
        """
        INSERT INTO delta_layout_append_partitioned VALUES
          (3, 'A', 30),
          (4, 'C', 40)
        """
      Given remember data files in location as files_v1
      Then data file lifecycle from files_v0 to files_v1 is append
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_append_partitioned
        """
      Then query result
        | count |
        | 4     |
      Then file tree in location matches
        """
        📂 category=A
          📄 part-<id>.<codec>.parquet
          📄 part-<id>.<codec>.parquet
        📂 category=B
          📄 part-<id>.<codec>.parquet
        📂 category=C
          📄 part-<id>.<codec>.parquet
        """

  Rule: Delta write options stored on table affect file splitting
    Background:
      Given variable location for temporary directory delta_file_layout_write_options
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_write_options
        """
      Given statement template
        """
        CREATE TABLE delta_layout_write_options (
          id INT,
          name STRING,
          description STRING,
          metadata STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (
          target_file_size '50000',
          write_batch_size '100'
        )
        """

    Scenario: Small target_file_size produces multiple data files
      Given statement
        """
        INSERT INTO delta_layout_write_options
        SELECT
          id,
          concat('user_with_very_long_name_', cast(id AS STRING), repeat('x', 200)) AS name,
          concat('description_', cast(id AS STRING), repeat('y', 200)) AS description,
          concat('metadata_', cast(id AS STRING), repeat('z', 200)) AS metadata
        FROM range(1, 1001)
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_write_options
        """
      Then query result
        | count |
        | 1000  |
      Then data files in location count is at least 3

  Rule: Data skipping setup produces multiple data files (for pruning tests)
    Background:
      Given variable location for temporary directory delta_file_layout_skipping
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_skipping_numeric
        """
      Given statement template
        """
        CREATE TABLE delta_layout_skipping_numeric (
          id BIGINT,
          value DOUBLE
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """

    Scenario: Three appends create three data files (numeric)
      Given statement
        """
        INSERT INTO delta_layout_skipping_numeric
        SELECT id, CAST(id AS DOUBLE) AS value FROM range(1, 11)
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_numeric
        SELECT id, CAST(id AS DOUBLE) AS value FROM range(100001, 100011)
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_numeric
        SELECT id, CAST(id AS DOUBLE) AS value FROM range(200001, 200011)
        """
      Then data files in location count is 3
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_skipping_numeric WHERE value > 200000.0
        """
      Then query result
        | count |
        | 10    |
      Then file tree in location matches
        """
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """

  Rule: Data skipping setup produces multiple data files (string/date)
    Background:
      Given variable location for temporary directory delta_file_layout_skipping_str_date
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_skipping_str_date
        """
      Given statement template
        """
        CREATE TABLE delta_layout_skipping_str_date (
          event_name STRING,
          event_date DATE
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """

    Scenario: Three appends create three data files (string/date)
      Given statement
        """
        INSERT INTO delta_layout_skipping_str_date VALUES
          ('A', DATE '2023-01-01'),
          ('B', DATE '2023-01-02'),
          ('C', DATE '2023-01-03')
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_str_date VALUES
          ('M', DATE '2023-06-01'),
          ('N', DATE '2023-06-02'),
          ('O', DATE '2023-06-03')
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_str_date VALUES
          ('X', DATE '2023-12-01'),
          ('Y', DATE '2023-12-02'),
          ('Z', DATE '2023-12-03')
        """
      Then data files in location count is 3
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_skipping_str_date WHERE event_name > 'W'
        """
      Then query result
        | count |
        | 3     |
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_skipping_str_date WHERE event_date < DATE '2023-03-01'
        """
      Then query result
        | count |
        | 3     |
      Then file tree in location matches
        """
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """

  Rule: Data skipping setup produces multiple data files (null counts)
    Background:
      Given variable location for temporary directory delta_file_layout_skipping_nulls
      Given final statement
        """
        DROP TABLE IF EXISTS delta_layout_skipping_nulls
        """
      Given statement template
        """
        CREATE TABLE delta_layout_skipping_nulls (
          id BIGINT,
          optional_col STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """

    Scenario: Three appends create three data files (null counts)
      Given statement
        """
        INSERT INTO delta_layout_skipping_nulls
        SELECT id, concat('value_', cast(id AS STRING)) AS optional_col FROM range(0, 10)
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_nulls
        SELECT id + 10 AS id, CAST(NULL AS STRING) AS optional_col FROM range(0, 10)
        """
      Given statement
        """
        INSERT INTO delta_layout_skipping_nulls
        SELECT
          id + 20 AS id,
          CASE WHEN id % 2 = 0 THEN concat('value_', cast(id AS STRING)) ELSE CAST(NULL AS STRING) END AS optional_col
        FROM range(0, 10)
        """
      Then data files in location count is 3
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_skipping_nulls WHERE optional_col IS NOT NULL
        """
      Then query result
        | count |
        | 15    |
      When query
        """
        SELECT COUNT(*) AS count FROM delta_layout_skipping_nulls WHERE optional_col IS NULL
        """
      Then query result
        | count |
        | 15    |
      Then file tree in location matches
        """
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        📄 part-<id>.<codec>.parquet
        """


