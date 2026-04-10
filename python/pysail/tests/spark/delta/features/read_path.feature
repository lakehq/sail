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

  @sail-only
  Rule: Driver path handles timestamp columns correctly

    Background:
      Given variable location for temporary directory delta_read_driver_timestamp
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_timestamp
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_timestamp (
          id INT,
          event_time TIMESTAMP
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_timestamp
        SELECT * FROM VALUES (1, TIMESTAMP '2024-01-01 00:00:00'), (2, TIMESTAMP '2024-06-01 12:00:00')
        """

    Scenario: EXPLAIN shows correct plan for driver path with timestamp column
      When query
        """
        EXPLAIN SELECT * FROM delta_read_driver_timestamp
        """
      Then query plan matches snapshot

    Scenario: SELECT returns correct rows from driver path table with timestamp column
      When query
        """
        SELECT id FROM delta_read_driver_timestamp ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

  @sail-only
  Rule: Driver path does not wrap RelaxedTzCastExec when there are no timestamp columns

    Background:
      Given variable location for temporary directory delta_read_driver_no_timestamp
      Given final statement
        """
        DROP TABLE IF EXISTS delta_read_driver_no_timestamp
        """
      Given statement template
        """
        CREATE TABLE delta_read_driver_no_timestamp (
          id INT,
          name STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_read_driver_no_timestamp
        SELECT * FROM VALUES (1, 'foo'), (2, 'bar')
        """

    Scenario: EXPLAIN omits RelaxedTzCastExec for driver path with no timestamp columns
      When query
        """
        EXPLAIN SELECT * FROM delta_read_driver_no_timestamp
        """
      Then query plan matches snapshot
