Feature: Delta Lake read path (driver vs metadata-as-data)

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
        CREATE TABLE delta_read_metadata_path (
          id INT,
          name STRING,
          value INT
        )
        USING DELTA LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_read_metadata_path
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """

    Scenario: EXPLAIN SELECT with metadataAsDataRead true uses discovery and log replay
      When query
        """
        EXPLAIN SELECT * FROM delta_read_metadata_path
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
