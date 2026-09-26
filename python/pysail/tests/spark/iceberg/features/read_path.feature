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

    Scenario: EXPLAIN SELECT with metadataAsDataRead true uses manifest scan
      Then iceberg metadata contains
        | path       | value |
        | properties | {}    |
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

  Rule: Metadata-as-data path returns the same rows as the driver path
    Background:
      Given variable location for temporary directory iceberg_read_metadata_rows
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_metadata_rows
        """
      Given statement template
        """
        CREATE TABLE iceberg_metadata_rows (
          id INT,
          name STRING,
          value INT
        )
        USING iceberg LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO iceberg_metadata_rows
        SELECT * FROM VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)
        """

    Scenario: SELECT through metadata-as-data path returns all rows
      When query
        """
        SELECT * FROM iceberg_metadata_rows ORDER BY id
        """
      Then query result ordered
        | id | name | value |
        | 1  | a    | 10    |
        | 2  | b    | 20    |
        | 3  | c    | 30    |

    Scenario: Filter and projection work on metadata-as-data path
      When query
        """
        SELECT name, value FROM iceberg_metadata_rows WHERE id >= 2 ORDER BY id
        """
      Then query result ordered
        | name | value |
        | b    | 20    |
        | c    | 30    |

    Scenario: Input file metadata works on the metadata-as-data path
      When query
        """
        SELECT id,
               length(input_file_name()) > 0 AS has_file,
               input_file_block_start() AS block_start,
               input_file_block_length() > 0 AS has_block
        FROM iceberg_metadata_rows
        ORDER BY id
        """
      Then query result ordered
        | id | has_file | block_start | has_block |
        | 1  | true     | 0           | true      |
        | 2  | true     | 0           | true      |
        | 3  | true     | 0           | true      |

  Rule: Identity-partition predicates remain correct on metadata-as-data path
    Background:
      Given variable location for temporary directory iceberg_read_metadata_part_filter
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_metadata_part_filter
        """
      Given statement template
        """
        CREATE TABLE iceberg_metadata_part_filter (
          id INT,
          category STRING,
          value INT
        )
        USING iceberg
        PARTITIONED BY (category)
        LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO iceberg_metadata_part_filter VALUES
          (1, 'a', 10),
          (2, 'a', 20),
          (3, 'b', 30),
          (4, 'b', 40),
          (5, 'c', 50)
        """

    Scenario: Identity-partition equality predicate is honored, not dropped
      When query
        """
        SELECT id, value FROM iceberg_metadata_part_filter WHERE category = 'b' ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 3  | 30    |
        | 4  | 40    |

  Rule: Transform partition predicates remain correct on metadata-as-data path
    Background:
      Given variable location for temporary directory iceberg_read_metadata_day_filter
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_metadata_day_filter
        """
      Given statement template
        """
        CREATE TABLE iceberg_metadata_day_filter (
          id INT,
          payload_timestamp TIMESTAMP,
          payload STRING
        )
        USING iceberg
        PARTITIONED BY (days(payload_timestamp))
        LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO iceberg_metadata_day_filter VALUES
          (1, TIMESTAMP '2024-01-01 10:00:00', 'day1-a'),
          (2, TIMESTAMP '2024-01-01 11:00:00', 'day1-b'),
          (3, TIMESTAMP '2024-01-02 10:00:00', 'day2-a'),
          (4, TIMESTAMP '2024-01-02 11:00:00', 'day2-b'),
          (5, TIMESTAMP '2024-01-03 10:00:00', 'day3-a'),
          (6, TIMESTAMP '2024-01-03 11:00:00', 'day3-b')
        """

    Scenario: Day-transform timestamp range predicate is honored, not dropped
      When query
        """
        SELECT id, payload FROM iceberg_metadata_day_filter
        WHERE payload_timestamp >= TIMESTAMP '2024-01-02 00:00:00'
          AND payload_timestamp < TIMESTAMP '2024-01-03 00:00:00'
        ORDER BY id
        """
      Then query result ordered
        | id | payload |
        | 3  | day2-a  |
        | 4  | day2-b  |

  Rule: Positional deletes preserve read filters and file row numbers
    Scenario Outline: Sorting unsorted files preserves physical delete positions
      Given variable location for temporary directory iceberg_unsorted_positions
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_unsorted_positions
        """
      Given statement template
        """
        CREATE TABLE iceberg_unsorted_positions (id BIGINT) USING iceberg
        LOCATION {{ location.uri }} TBLPROPERTIES (
          'format-version' = '<version>', 'write.merge.mode' = 'merge-on-read')
        """
      Given statement
        """
        INSERT INTO iceberg_unsorted_positions
        SELECT /*+ COALESCE(1) */ id FROM VALUES (8L), (19L), (2L), (7L), (3L) AS data(id)
        ORDER BY id DESC
        """
      Given statement
        """
        MERGE INTO iceberg_unsorted_positions t
        USING (SELECT * FROM VALUES (2L), (7L) AS deleted(id)) s
        ON t.id = s.id WHEN MATCHED THEN DELETE
        """
      Given statement
        """
        INSERT INTO iceberg_unsorted_positions VALUES (30L)
        """
      When query
        """
        SELECT id FROM iceberg_unsorted_positions ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 8  |
        | 19 |
        | 30 |

      Examples:
        | version |
        | 2       |
        | 3       |

    Scenario Outline: Positional deletes apply before projections and limits
      Given variable location for temporary directory iceberg_position_selection
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_position_selection
        """
      Given statement template
        """
        CREATE TABLE iceberg_position_selection (id BIGINT) USING iceberg
        LOCATION {{ location.uri }} TBLPROPERTIES (
          'format-version' = '<version>', 'write.merge.mode' = 'merge-on-read')
        """
      Given statement
        """
        INSERT INTO iceberg_position_selection
        SELECT /*+ COALESCE(1) */ id FROM range(1024)
        """
      Given statement
        """
        MERGE INTO iceberg_position_selection t
        USING (SELECT id FROM range(1024) WHERE id < 128 OR id IN (511, 512, 513, 600, 1023)) s
        ON t.id = s.id WHEN MATCHED THEN DELETE
        """
      When query
        """
        SELECT count(*) AS total, sum(id) AS sum_ids FROM iceberg_position_selection
        """
      Then query result
        | total | sum_ids |
        | 891   | 512489  |
      When query
        """
        SELECT id FROM iceberg_position_selection WHERE id >= 510 ORDER BY id LIMIT 3
        """
      Then query result ordered
        | id  |
        | 510 |
        | 514 |
        | 515 |
      When query
        """
        SELECT count(*) AS total FROM (SELECT 1 FROM iceberg_position_selection LIMIT 3)
        """
      Then query result
        | total |
        | 3     |
      Given statement
        """
        MERGE INTO iceberg_position_selection t USING (SELECT 514L AS id) s
        ON t.id = s.id WHEN MATCHED THEN DELETE
        """
      When query
        """
        SELECT id FROM iceberg_position_selection WHERE id BETWEEN 510 AND 516 ORDER BY id
        """
      Then query result ordered
        | id  |
        | 510 |
        | 515 |
        | 516 |

      Examples:
        | version |
        | 2       |
        | 3       |
