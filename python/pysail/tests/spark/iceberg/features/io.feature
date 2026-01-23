Feature: Iceberg Basic IO

  Rule: Create table and basic operations
    Background:
      Given variable location for temporary directory iceberg_io
      Given final statement
        """
        DROP TABLE IF EXISTS test_table
        """

    Scenario: Create a new table and append data
      Given statement template
        """
        CREATE TABLE test_table (id INT, data STRING) 
        USING iceberg 
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'a'), (2, 'b')
        """
      Then iceberg metadata contains current snapshot
      Then iceberg snapshot operation is append
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | data |
        | 1  | a    |
        | 2  | b    |

    Scenario: Multiple inserts create multiple snapshots
      Given statement template
        """
        CREATE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'first')
        """
      Then iceberg snapshot count is 1
      Given statement
        """
        INSERT INTO test_table VALUES (2, 'second')
        """
      Then iceberg snapshot count is 2
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | first  |
        | 2  | second |

  Rule: Verify file layout for unpartitioned tables
    Background:
      Given variable location for temporary directory iceberg_io_layout
      Given final statement
        """
        DROP TABLE IF EXISTS unpart_table
        """

    Scenario: Unpartitioned table writes to data directory
      Given statement template
        """
        CREATE TABLE unpart_table (id INT, name STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO unpart_table VALUES (1, 'alice'), (2, 'bob')
        """
      Then file tree in location matches
        """
        📂 data
          📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """

  Rule: Overwrite data creates new snapshot
    Background:
      Given variable location for temporary directory iceberg_io_overwrite
      Given final statement
        """
        DROP TABLE IF EXISTS overwrite_table
        """

    Scenario: INSERT OVERWRITE replaces data
      Given statement template
        """
        CREATE TABLE overwrite_table (id INT, status STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO overwrite_table VALUES (1, 'old'), (2, 'old')
        """
      Then iceberg snapshot operation is append
      Given statement
        """
        INSERT OVERWRITE TABLE overwrite_table VALUES (3, 'new'), (4, 'new')
        """
      Then iceberg snapshot operation is overwrite
      Then iceberg snapshot count is 2
      When query
        """
        SELECT * FROM overwrite_table ORDER BY id
        """
      Then query result ordered
        | id | status |
        | 3  | new    |
        | 4  | new    |
