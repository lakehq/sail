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
