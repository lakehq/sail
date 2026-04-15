Feature: Iceberg read path (driver vs metadata-as-data)

  Rule: EXPLAIN shows standard driver path when table has no metadataAsDataRead option
    Background:
      Given variable location for temporary directory iceberg_read_driver
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_read_driver_path
        """
      Given statement template
        """
        CREATE TABLE iceberg_read_driver_path (
          id INT,
          name STRING,
          value INT
        )
        USING iceberg LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_read_driver_path
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """

    @sail-only
    Scenario: EXPLAIN SELECT with default options uses driver file scan
      When query
        """
        EXPLAIN SELECT * FROM iceberg_read_driver_path
        """
      Then query plan matches snapshot

  Rule: EXPLAIN shows metadata-as-data path when table has metadataAsDataRead option
    Background:
      Given variable location for temporary directory iceberg_read_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_read_metadata_path
        """
      Given statement template
        """
        CREATE TABLE iceberg_read_metadata_path (
          id INT,
          name STRING,
          value INT
        )
        USING iceberg LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO iceberg_read_metadata_path
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20)
        """

    @sail-only
    Scenario: EXPLAIN SELECT with metadataAsDataRead true uses manifest scan
      When query
        """
        EXPLAIN SELECT * FROM iceberg_read_metadata_path
        """
      Then query plan matches snapshot

  Rule: Metadata-as-data with partitioned table
    Background:
      Given variable location for temporary directory iceberg_read_metadata_partitioned
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_metadata_partitioned
        """

    @sail-only
    Scenario: EXPLAIN for partitioned table with metadata-as-data enabled
      Given statement template
        """
        CREATE TABLE iceberg_metadata_partitioned (
          id INT,
          category INT,
          value STRING
        )
        USING iceberg
        PARTITIONED BY (category)
        LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO iceberg_metadata_partitioned VALUES
          (1, 1, 'cat1'), (2, 1, 'cat1'),
          (3, 2, 'cat2'), (4, 2, 'cat2')
        """
      When query
        """
        EXPLAIN SELECT * FROM iceberg_metadata_partitioned
        """
      Then query plan matches snapshot
