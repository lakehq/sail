Feature: Delta Lake read path (driver vs metadata-as-data)

  Rule: Exact partition filters preserve logical types and three-valued logic
    Background:
      Given variable location for temporary directory typed_partition_filters
      Given final statement
        """
        DROP TABLE IF EXISTS typed_partition_filters
        """

    Scenario Outline: Partition and data filters compose without losing exact conditions
      Given statement template
        """
        CREATE TABLE typed_partition_filters (p INT, q INT, v INT)
        USING DELTA PARTITIONED BY (p, q) LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead '<metadata_as_data>')
        """
      Given statement
        """
        INSERT INTO typed_partition_filters VALUES
          (10, 2, 100), (10, 2, -1), (2, 10, 200), (3, 2, 300), (1, 1, 1), (NULL, 2, 400)
        """
      When query
        """
        SELECT p, q, v FROM typed_partition_filters WHERE p > q AND v > 0 ORDER BY v
        """
      Then query result collected ordered
        | p  | q | v   |
        | 10 | 2 | 100 |
        | 3  | 2 | 300 |
      When query
        """
        SELECT p, COUNT(*) AS n FROM typed_partition_filters
        WHERE NOT (p <= q) GROUP BY p ORDER BY p
        """
      Then query result collected ordered
        | p  | n |
        | 3  | 1 |
        | 10 | 2 |
      When query
        """
        SELECT COUNT(*) AS n FROM typed_partition_filters WHERE p NOT IN (1, NULL)
        """
      Then query result collected
        | n |
        | 0 |
      When query
        """
        SELECT v FROM typed_partition_filters WHERE p > q OR v = 200 ORDER BY v
        """
      Then query result collected ordered
        | v   |
        | -1  |
        | 100 |
        | 200 |
        | 300 |
      When query
        """
        EXPLAIN SELECT p, q, v FROM typed_partition_filters WHERE p > q AND v > 0 ORDER BY v
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT p, COUNT(*) AS n FROM typed_partition_filters
        WHERE NOT (p <= q) GROUP BY p ORDER BY p
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT COUNT(*) AS n FROM typed_partition_filters WHERE p NOT IN (1, NULL)
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT v FROM typed_partition_filters WHERE p > q OR v = 200 ORDER BY v
        """
      Then query plan matches snapshot

      Examples:
        | metadata_as_data |
        | false            |
        | true             |

  Rule: Partition metadata aggregation works through view aliases
    Background:
      Given variable location for temporary directory delta_metadata_grouping
      Given final statement
        """
        DROP VIEW IF EXISTS metadata_grouping_alias
        """
      Given final statement
        """
        DROP TABLE IF EXISTS delta_metadata_grouping
        """

    Scenario Outline: Partition metadata aggregation preserves counts and distinct values
      Given statement template
        """
        CREATE TABLE delta_metadata_grouping (id INT, part STRING)
        USING DELTA PARTITIONED BY (part) LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead '<metadata_as_data>')
        """
      Given statement
        """
        INSERT INTO delta_metadata_grouping VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, NULL), (5, NULL)
        """
      Given statement
        """
        CREATE TEMP VIEW metadata_grouping_alias AS SELECT part AS p FROM delta_metadata_grouping
        """
      When query
        """
        SELECT p, COUNT(*) AS n FROM metadata_grouping_alias GROUP BY p ORDER BY p
        """
      Then query result collected ordered
        | p    | n |
        | NULL | 2 |
        | a    | 2 |
        | b    | 1 |
      When query
        """
        SELECT DISTINCT p FROM metadata_grouping_alias ORDER BY p
        """
      Then query result collected ordered
        | p    |
        | NULL |
        | a    |
        | b    |
      When query
        """
        SELECT COUNT(*) AS n FROM metadata_grouping_alias
        """
      Then query result collected ordered
        | n |
        | 5 |
      When query
        """
        EXPLAIN SELECT p, COUNT(*) AS n FROM metadata_grouping_alias GROUP BY p
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT DISTINCT p FROM metadata_grouping_alias
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT COUNT(*) AS n FROM metadata_grouping_alias
        """
      Then query plan matches snapshot

      Examples:
        | metadata_as_data |
        | false            |
        | true             |

    Scenario: Partition filtered extrema and bounded row existence use metadata
      Given statement template
        """
        CREATE TABLE delta_metadata_grouping (id INT, part STRING)
        USING DELTA PARTITIONED BY (part) LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_metadata_grouping VALUES (1, 'a'), (2, 'a'), (3, 'b'), (NULL, 'a')
        """
      Given statement
        """
        CREATE TEMP VIEW metadata_grouping_alias AS SELECT id AS value, part AS p FROM delta_metadata_grouping
        """
      When query
        """
        SELECT COUNT(*) AS n, COUNT(value) AS nonnull, MIN(value) AS lo, MAX(value) AS hi
        FROM metadata_grouping_alias WHERE p = 'a'
        """
      Then query result collected
        | n | nonnull | lo | hi |
        | 3 | 2       | 1  | 2  |
      When query
        """
        SELECT 1 AS present FROM metadata_grouping_alias WHERE p = 'a' LIMIT 2 OFFSET 1
        """
      Then query result collected
        | present |
        | 1       |
        | 1       |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS n, COUNT(value) AS nonnull, MIN(value) AS lo, MAX(value) AS hi
        FROM metadata_grouping_alias WHERE p = 'a'
        """
      Then query plan matches snapshot
      When query
        """
        EXPLAIN SELECT 1 AS present FROM metadata_grouping_alias WHERE p = 'a' LIMIT 2 OFFSET 1
        """
      Then query plan matches snapshot

  Rule: EXPLAIN shows driver path when table has no metadataAsDataRead option
    Background:
      Given variable location for temporary directory delta_read_driver
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_path
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_path (
          id INT,
          name STRING,
          value INT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_path
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """

    Scenario: EXPLAIN SELECT with default options uses driver file scan
      When query
        """
        EXPLAIN SELECT * FROM delta_read_driver_path
        """
      Then query plan matches snapshot

  Rule: EXPLAIN shows timestamp schema adaptation inside the driver file scan
    Background:
      Given variable location for temporary directory delta_read_driver_timestamp
      Given config spark.sql.session.timeZone = America/Los_Angeles
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_timestamp_path
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_timestamp_path (
          id INT,
          event_time TIMESTAMP
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_timestamp_path
        VALUES (1, TIMESTAMP '2024-05-01 12:00:00')
        """

    Scenario: EXPLAIN SELECT with timestamp uses the standard driver file scan
      When query
        """
        EXPLAIN SELECT * FROM delta_read_driver_timestamp_path
        """
      Then query plan matches snapshot

    Scenario: SELECT with timestamp preserves the session-local value
      When query
        """
        SELECT id, CAST(event_time AS STRING) AS event_time
        FROM delta_read_driver_timestamp_path
        """
      Then query result ordered
        | id | event_time          |
        | 1  | 2024-05-01 12:00:00 |

  Rule: EXPLAIN shows metadata-as-data path when table has metadataAsDataRead option
    Background:
      Given variable location for temporary directory delta_read_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_metadata_path
        """
      Given statement template
        """
        CREATE TABLE delta_read_metadata_path
        USING DELTA LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        AS SELECT * FROM VALUES
          (1, 'a', 10),
          (2, 'b', 20)
        AS t(id, name, value)
        """

    Scenario: EXPLAIN SELECT with metadataAsDataRead true uses discovery and log replay
      When query
        """
        EXPLAIN SELECT * FROM delta_read_metadata_path
        """
      Then query plan matches snapshot

    Scenario: SQL row existence preserves metadata replay and filters
      When query
        """
        SELECT 1 AS present FROM delta_read_metadata_path LIMIT 1
        """
      Then query result collected
        | present |
        | 1       |
      When query
        """
        SELECT 1 AS present FROM delta_read_metadata_path WHERE id > 2 LIMIT 1
        """
      Then query result collected
        | present |
      When query
        """
        SELECT 1 AS present
        WHERE EXISTS(SELECT * FROM delta_read_metadata_path WHERE id > 2)
        """
      Then query result collected
        | present |
      When query
        """
        EXPLAIN SELECT 1 AS present FROM delta_read_metadata_path LIMIT 1
        """
      Then query plan matches snapshot

    Scenario: Metadata pruning does not assume arbitrary casts preserve bounds
      Given statement
        """
        INSERT INTO delta_read_metadata_path VALUES (3, 'c', 2), (4, 'd', 10)
        """
      When query
        """
        SELECT id
        FROM delta_read_metadata_path
        WHERE CAST(value AS STRING) < '2'
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 4  |

    Scenario: EXPLAIN CODEGEN shows the distributed metadata replay stages
      When query
        """
        EXPLAIN CODEGEN SELECT * FROM delta_read_metadata_path
        """
      Then query plan matches snapshot

  Rule: EXPLAIN shows partition-pruned driver path with default options
    Background:
      Given variable location for temporary directory delta_read_driver_partitioned
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_partitioned_path
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_partitioned_path (
          id INT,
          year INT,
          value INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_partitioned_path
        SELECT * FROM VALUES (1, 2023, 10), (2, 2023, 20), (3, 2024, 30), (4, 2024, 40)
        """

    Scenario: EXPLAIN SELECT with default options and partition filter prunes driver file scan
      When query
        """
        EXPLAIN SELECT * FROM delta_read_driver_partitioned_path WHERE year = 2024
        """
      Then query plan matches snapshot

    Scenario: SQL row existence preserves partition predicates
      When query
        """
        SELECT 1 AS present FROM delta_read_driver_partitioned_path WHERE year = 2024 LIMIT 1
        """
      Then query result collected
        | present |
        | 1       |
      When query
        """
        SELECT 1 AS present FROM delta_read_driver_partitioned_path WHERE year = 2025 LIMIT 1
        """
      Then query result collected
        | present |
      When query
        """
        EXPLAIN SELECT 1 AS present FROM delta_read_driver_partitioned_path WHERE year = 2024 LIMIT 1
        """
      Then query plan matches snapshot

  Rule: Append-only table with no remove actions is readable on driver path
    Background:
      Given variable location for temporary directory delta_read_driver_append_only
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_append_only
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_append_only (
          id INT,
          name STRING,
          value INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_append_only
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """
      Given statement
        """
        INSERT INTO delta_read_driver_append_only
        SELECT * FROM VALUES (3, 'c', 30), (4, 'd', 40)
        """

    Scenario: SELECT succeeds after append-only writes with metadataAsDataRead disabled
      When query
        """
        SELECT id, name, value FROM delta_read_driver_append_only ORDER BY id
        """
      Then query result ordered
        | id | name | value |
        | 1  | a    | 10    |
        | 2  | b    | 20    |
        | 3  | c    | 30    |
        | 4  | d    | 40    |

  Rule: Grouped counts use constant-file statistics on the driver path
    Background:
      Given variable location for temporary directory delta_grouped_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_grouped_metadata_count
        """
      Given statement template
        """
        CREATE TABLE delta_grouped_metadata_count (
          id INT,
          provider STRING,
          file_id INT
        )
        USING DELTA
        PARTITIONED BY (file_id)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_grouped_metadata_count VALUES
          (1, 'fb', 1),
          (2, 'fb', 1),
          (3, 'yt', 2),
          (4, 'yt', 2),
          (5, 'yt', 2),
          (6, 'in', 3),
          (7, 'fb', 4),
          (8, 'in', 4)
        """

    Scenario: Grouped count combines metadata-only and residual files
      When query
        """
        SELECT COUNT(1) AS cnt, provider
        FROM delta_grouped_metadata_count
        GROUP BY provider
        ORDER BY provider
        """
      Then query result ordered
        | cnt | provider |
        | 3   | fb       |
        | 2   | in       |
        | 3   | yt       |

    Scenario: EXPLAIN grouped count shows metadata rows and a residual scan
      When query
        """
        EXPLAIN SELECT COUNT(1) AS cnt, provider
        FROM delta_grouped_metadata_count
        GROUP BY provider
        """
      Then query plan matches snapshot

  Rule: Exact Delta aggregates consume typed snapshot statistics
    Background:
      Given variable location for temporary directory delta_exact_aggregates
      Given final statement
        """
        DROP TABLE IF EXISTS delta_exact_aggregates
        """
      Given statement template
        """
        CREATE TABLE delta_exact_aggregates (
          id INT,
          nullable_value INT,
          other_nullable INT,
          all_null INT,
          payload STRUCT<score: INT>,
          part STRING
        )
        USING DELTA
        PARTITIONED BY (part)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_exact_aggregates VALUES
          (1, 10, NULL, NULL, named_struct('score', 9), '10'),
          (2, NULL, 20, NULL, named_struct('score', 5), '2'),
          (3, 30, 30, NULL, named_struct('score', 7), '2')
        """

    Scenario: Counts and extrema use row, partition, nested, literal, and cast statistics
      When query
        """
        SELECT
          COUNT(*) AS rows,
          COUNT(nullable_value) AS present_values,
          COUNT(part) AS present_parts,
          MIN(id) AS min_id,
          MAX(id) AS max_id,
          MIN(part) AS min_part,
          MAX(part) AS max_part,
          MIN(payload.score) AS min_score,
          MAX(payload.score) AS max_score,
          MIN(42) AS min_literal,
          MAX(42) AS max_literal,
          MIN(all_null) AS min_null,
          MAX(all_null) AS max_null
        FROM delta_exact_aggregates
        """
      Then query result
        | rows | present_values | present_parts | min_id | max_id | min_part | max_part | min_score | max_score | min_literal | max_literal | min_null | max_null |
        | 3    | 2              | 3             | 1      | 3      | 10       | 2        | 5         | 9         | 42          | 42          | NULL     | NULL     |
      When query
        """
        SELECT
          MIN(CAST(id AS BIGINT)) AS min_id_long,
          MAX(CAST(id AS BIGINT)) AS max_id_long
        FROM delta_exact_aggregates
        """
      Then query result
        | min_id_long | max_id_long |
        | 1           | 3           |

    Scenario: EXPLAIN exact aggregates contains no Delta data scan
      When query
        """
        EXPLAIN SELECT COUNT(*), COUNT(nullable_value), MIN(id), MAX(payload.score)
        FROM delta_exact_aggregates
        """
      Then query plan matches snapshot

    Scenario: Exact and residual aggregates share one global aggregate row
      When query
        """
        SELECT
          COUNT(*) AS rows,
          MIN(id) AS minimum,
          SUM(nullable_value) AS total
        FROM delta_exact_aggregates
        """
      Then query result
        | rows | minimum | total |
        | 3    | 1       | 40    |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS rows, MIN(id) AS minimum, SUM(nullable_value) AS total
        FROM delta_exact_aggregates
        """
      Then query plan matches snapshot

    Scenario: Distinct literal counts use snapshot cardinality
      When query
        """
        SELECT COUNT(DISTINCT 1) AS one_count, COUNT(DISTINCT NULL) AS null_count
        FROM delta_exact_aggregates
        """
      Then query result
        | one_count | null_count |
        | 1         | 0          |
      When query
        """
        EXPLAIN SELECT COUNT(DISTINCT 1) AS one_count
        FROM delta_exact_aggregates
        """
      Then query plan matches snapshot
      Given statement
        """
        DELETE FROM delta_exact_aggregates WHERE TRUE
        """
      When query
        """
        SELECT COUNT(DISTINCT 1) AS one_count, COUNT(DISTINCT NULL) AS null_count
        FROM delta_exact_aggregates
        """
      Then query result
        | one_count | null_count |
        | 0         | 0          |

    Scenario: Independent nullable arguments retain row evaluation
      When query
        """
        SELECT COUNT(nullable_value, other_nullable) AS joint_count
        FROM delta_exact_aggregates
        """
      Then query result
        | joint_count |
        | 1           |
      When query
        """
        EXPLAIN SELECT COUNT(nullable_value, other_nullable) AS joint_count
        FROM delta_exact_aggregates
        """
      Then query plan matches snapshot

    Scenario: Non-monotonic cast extrema retain the scan
      When query
        """
        SELECT MIN(CAST(part AS INT)) AS minimum, MAX(CAST(part AS INT)) AS maximum
        FROM delta_exact_aggregates
        """
      Then query result
        | minimum | maximum |
        | 2       | 10      |

  Rule: Exact Delta aggregates preserve decimal bounds
    Background:
      Given variable location for temporary directory delta_exact_writer_stats
      Given final statement
        """
        DROP TABLE IF EXISTS delta_exact_writer_stats
        """
      Given statement template
        """
        CREATE TABLE delta_exact_writer_stats (
          amount DECIMAL(18, 0)
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_exact_writer_stats VALUES
          (CAST('9007199254740993' AS DECIMAL(18, 0))),
          (CAST('9007199254740995' AS DECIMAL(18, 0)))
        """

    Scenario: Decimal extrema retain values beyond exact floating point range
      When query
        """
        SELECT MIN(amount) AS minimum, SUM(amount) AS total
        FROM delta_exact_writer_stats
        """
      Then query result
        | minimum          | total             |
        | 9007199254740993 | 18014398509481988 |

  Rule: Append-only table with no remove actions is readable on metadata-as-data path
    Background:
      Given variable location for temporary directory delta_read_metadata_append_only
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_metadata_append_only
        """
      Given statement template
        """
        CREATE TABLE delta_read_metadata_append_only (
          id INT,
          name STRING,
          value INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead true)
        """
      Given statement
        """
        INSERT INTO delta_read_metadata_append_only
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """
      Given statement
        """
        INSERT INTO delta_read_metadata_append_only
        SELECT * FROM VALUES (3, 'c', 30), (4, 'd', 40)
        """

    Scenario: SELECT succeeds after append-only writes with metadataAsDataRead enabled
      When query
        """
        SELECT id, name, value FROM delta_read_metadata_append_only ORDER BY id
        """
      Then query result ordered
        | id | name | value |
        | 1  | a    | 10    |
        | 2  | b    | 20    |
        | 3  | c    | 30    |
        | 4  | d    | 40    |

  Rule: COUNT(1) uses exact Delta row-count metadata but scans CSV data
    Background:
      Given variable delta_count_location for temporary directory delta_count_metadata_only
      Given variable csv_count_location for temporary directory csv_count_scan
      Given final statement
        """
        DROP TABLE IF EXISTS delta_count_metadata_only
        """
      Given final statement
        """
        DROP TABLE IF EXISTS csv_count_scan
        """
      Given statement template
        """
        CREATE TABLE delta_count_metadata_only (id INT)
        USING DELTA LOCATION {{ delta_count_location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_count_metadata_only VALUES (1), (2), (3)
        """
      Given statement template
        """
        CREATE TABLE csv_count_scan (id INT)
        USING CSV LOCATION {{ csv_count_location.sql }}
        """
      Given statement
        """
        INSERT INTO csv_count_scan VALUES (1), (2), (3)
        """

    Scenario: EXPLAIN COUNT(1) on Delta replaces the data scan with metadata
      When query
        """
        EXPLAIN SELECT COUNT(1) AS cnt FROM delta_count_metadata_only
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN COUNT(1) on CSV retains the file scan
      When query
        """
        EXPLAIN SELECT COUNT(1) AS cnt FROM csv_count_scan
        """
      Then query plan matches snapshot

  Rule: SQL row existence uses Delta row counts without column statistics
    Background:
      Given variable location for temporary directory delta_row_existence
      Given final statement
        """
        DROP TABLE IF EXISTS delta_row_existence
        """
      Given statement template
        """
        CREATE TABLE delta_row_existence (id INT, value INT)
        USING DELTA LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')
        """

    Scenario: SQL row existence returns no row for an empty Delta snapshot
      When query
        """
        SELECT 1 AS present FROM delta_row_existence LIMIT 1
        """
      Then query result collected
        | present |
      Then query schema
        """
        root
         |-- present: integer (nullable = false)
        """
      When query
        """
        EXPLAIN SELECT 1 AS present FROM delta_row_existence LIMIT 1
        """
      Then query plan matches snapshot

    Scenario: SQL row existence uses metadata for a nonempty Delta snapshot
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (1, 10), (2, NULL), (NULL, NULL)
        """
      When query
        """
        SELECT 1 AS present FROM delta_row_existence LIMIT 1
        """
      Then query result collected
        | present |
        | 1       |
      Then query schema
        """
        root
         |-- present: integer (nullable = false)
        """
      When query
        """
        EXPLAIN SELECT 1 AS present FROM delta_row_existence LIMIT 1
        """
      Then query plan matches snapshot

    Scenario: SQL row existence distinguishes an empty snapshot from an all-null row
      When query
        """
        SELECT 1 AS present WHERE EXISTS(SELECT * FROM delta_row_existence)
        """
      Then query result collected
        | present |
      When query
        """
        SELECT 1 AS absent WHERE NOT EXISTS(SELECT * FROM delta_row_existence)
        """
      Then query result collected
        | absent |
        | 1      |
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (NULL, NULL)
        """
      When query
        """
        SELECT 1 AS present WHERE EXISTS(SELECT * FROM delta_row_existence)
        """
      Then query result collected
        | present |
        | 1       |
      When query
        """
        SELECT 1 AS absent WHERE NOT EXISTS(SELECT * FROM delta_row_existence)
        """
      Then query result collected
        | absent |
      When query
        """
        SELECT 1 AS present FROM delta_row_existence LIMIT 1
        """
      Then query result collected
        | present |
        | 1       |

    Scenario Outline: SQL row existence preserves LIMIT and OFFSET cardinality
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (1, 10), (2, NULL), (3, 30)
        """
      When query
        """
        SELECT COUNT(*) AS row_count
        FROM (SELECT 1 FROM delta_row_existence LIMIT <limit> OFFSET <offset>)
        """
      Then query result collected
        | row_count |
        | <count>   |

      Examples:
        | limit | offset | count |
        | 0     | 0      | 0     |
        | 2     | 0      | 2     |
        | 5     | 0      | 3     |
        | 1     | 1      | 1     |
        | 1     | 3      | 0     |
        | 2     | 2      | 1     |

    Scenario: SQL row existence preserves data predicates and ordered column reads
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (3, 30), (1, 10), (2, NULL)
        """
      When query
        """
        SELECT 1 AS present FROM delta_row_existence WHERE id > 3 LIMIT 1
        """
      Then query result collected
        | present |
      When query
        """
        SELECT 1 AS present FROM delta_row_existence WHERE id = 2 LIMIT 1
        """
      Then query result collected
        | present |
        | 1       |
      When query
        """
        SELECT id FROM delta_row_existence ORDER BY id DESC LIMIT 1
        """
      Then query result collected
        | id |
        | 3  |
      When query
        """
        EXPLAIN SELECT 1 AS present FROM delta_row_existence WHERE id = 2 LIMIT 1
        """
      Then query plan matches snapshot

    Scenario: SQL row existence preserves correlated subquery predicates
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (1, 10), (2, NULL), (3, 30)
        """
      When query
        """
        SELECT candidate.id
        FROM VALUES (2), (4) AS candidate(id)
        WHERE EXISTS(
          SELECT 1 FROM delta_row_existence
          WHERE delta_row_existence.id = candidate.id
        )
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id |
        | 2  |
      When query
        """
        SELECT candidate.id
        FROM VALUES (2), (4) AS candidate(id)
        WHERE NOT EXISTS(
          SELECT 1 FROM delta_row_existence
          WHERE delta_row_existence.id = candidate.id
        )
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id |
        | 4  |

      When query
        """
        SELECT candidate.id,
          EXISTS(
            SELECT 1 FROM delta_row_existence
            WHERE delta_row_existence.id = candidate.id
          ) AS present,
          NOT EXISTS(
            SELECT 1 FROM delta_row_existence
            WHERE delta_row_existence.id = candidate.id
          ) AS absent
        FROM VALUES (2), (4) AS candidate(id)
        ORDER BY candidate.id
        """
      Then query result collected ordered
        | id | present | absent |
        | 2  | true    | false  |
        | 4  | false   | true   |

    Scenario: SQL projected EXISTS returns row existence as a boolean
      When query
        """
        SELECT
          EXISTS(SELECT * FROM delta_row_existence) AS present,
          NOT EXISTS(SELECT * FROM delta_row_existence) AS absent
        """
      Then query result collected
        | present | absent |
        | false   | true   |
      Given statement
        """
        INSERT INTO delta_row_existence VALUES (1, 10)
        """
      When query
        """
        SELECT
          EXISTS(SELECT * FROM delta_row_existence) AS present,
          NOT EXISTS(SELECT * FROM delta_row_existence) AS absent
        """
      Then query result collected
        | present | absent |
        | true    | false  |
      Then query schema
        """
        root
         |-- present: boolean (nullable = false)
         |-- absent: boolean (nullable = false)
        """
      When query
        """
        SELECT
          EXISTS(SELECT * FROM delta_row_existence WHERE id > 1) AS present,
          NOT EXISTS(SELECT * FROM delta_row_existence WHERE id > 1) AS absent
        """
      Then query result collected
        | present | absent |
        | false   | true   |
      When query
        """
        EXPLAIN SELECT EXISTS(SELECT * FROM delta_row_existence) AS present
        """
      Then query plan matches snapshot
