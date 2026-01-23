Feature: Iceberg Partitioning

  Rule: Identity partitioning creates partition directories
    Background:
      Given variable location for temporary directory iceberg_part_identity
      Given final statement
        """
        DROP TABLE IF EXISTS part_identity
        """

    Scenario: Partition by identity transform on integer column
      Given statement template
        """
        CREATE TABLE part_identity (id INT, category INT, value STRING)
        USING iceberg
        PARTITIONED BY (category)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO part_identity VALUES 
          (1, 1, 'cat1_val1'), 
          (2, 1, 'cat1_val2'),
          (3, 2, 'cat2_val1'),
          (4, 2, 'cat2_val2')
        """
      Then iceberg snapshot operation is append
      When query
        """
        SELECT * FROM part_identity ORDER BY id
        """
      Then query result ordered
        | id | value     | category |
        | 1  | cat1_val1 | 1        |
        | 2  | cat1_val2 | 1        |
        | 3  | cat2_val1 | 2        |
        | 4  | cat2_val2 | 2        |
      Then file tree in location matches
        """
        📂 data
          📂 category=1
            📄 *.parquet
          📂 category=2
            📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """

  Rule: Multi-column partitioning creates nested structure
    Background:
      Given variable location for temporary directory iceberg_part_multi
      Given final statement
        """
        DROP TABLE IF EXISTS part_multi
        """

    Scenario: Partition by multiple identity columns
      Given statement template
        """
        CREATE TABLE part_multi (id INT, region INT, category INT, value INT)
        USING iceberg
        PARTITIONED BY (region, category)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO part_multi VALUES
          (1, 1, 1, 100),
          (2, 1, 2, 200),
          (3, 2, 1, 300),
          (4, 2, 2, 400)
        """
      When query
        """
        SELECT * FROM part_multi ORDER BY id
        """
      Then query result ordered
        | id | value | region | category |
        | 1  | 100   | 1      | 1        |
        | 2  | 200   | 1      | 2        |
        | 3  | 300   | 2      | 1        |
        | 4  | 400   | 2      | 2        |
      Then file tree in location matches
        """
        📂 data
          📂 region=1
            📂 category=1
              📄 *.parquet
            📂 category=2
              📄 *.parquet
          📂 region=2
            📂 category=1
              📄 *.parquet
            📂 category=2
              📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """

  Rule: Bucket transform partitioning
    Background:
      Given variable location for temporary directory iceberg_part_bucket
      Given final statement
        """
        DROP TABLE IF EXISTS part_bucket
        """

    Scenario: Partition by bucket transform
      Given statement template
        """
        CREATE TABLE part_bucket (id INT, val STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('write.partitioning'='bucket[4](id)')
        """
      Given statement
        """
        INSERT INTO part_bucket VALUES 
          (1, 'v1'), (2, 'v2'), (3, 'v3'), (4, 'v4'),
          (5, 'v5'), (6, 'v6'), (7, 'v7'), (8, 'v8')
        """
      Then iceberg metadata contains current snapshot
      When query
        """
        SELECT COUNT(*) as count FROM part_bucket
        """
      Then query result
        | count |
        | 8     |
      # Note: bucket transform creates id_bucket=N directories
      Then data files in location count is at least 1

  Rule: Truncate transform partitioning
    Background:
      Given variable location for temporary directory iceberg_part_truncate
      Given final statement
        """
        DROP TABLE IF EXISTS part_truncate
        """

    Scenario: Partition by truncate transform on string
      Given statement template
        """
        CREATE TABLE part_truncate (id INT, code STRING, value INT)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('write.partitioning'='truncate[3](name)')
        """
      Given statement
        """
        INSERT INTO part_truncate VALUES
          (1, 'AAAA', 100),
          (2, 'AABB', 200),
          (3, 'BBAA', 300),
          (4, 'BBBB', 400)
        """
      Then iceberg metadata contains current snapshot
      When query
        """
        SELECT * FROM part_truncate ORDER BY id
        """
      Then query result ordered
        | id | code | value |
        | 1  | AAAA | 100   |
        | 2  | AABB | 200   |
        | 3  | BBAA | 300   |
        | 4  | BBBB | 400   |

  Rule: Year/Month/Day transforms on timestamp
    Background:
      Given variable location for temporary directory iceberg_part_temporal
      Given final statement
        """
        DROP TABLE IF EXISTS part_temporal
        """

    Scenario: Partition by year transform on timestamp
      Given statement template
        """
        CREATE TABLE part_temporal (id INT, event_time TIMESTAMP, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('write.partitioning'='year(event_time)')
        """
      Given statement
        """
        INSERT INTO part_temporal VALUES
          (1, TIMESTAMP '2023-01-15 10:00:00', 'event1'),
          (2, TIMESTAMP '2023-06-20 14:30:00', 'event2'),
          (3, TIMESTAMP '2024-03-10 09:15:00', 'event3'),
          (4, TIMESTAMP '2024-12-25 18:45:00', 'event4')
        """
      Then iceberg metadata contains current snapshot
      When query
        """
        SELECT id, value FROM part_temporal ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | event1 |
        | 2  | event2 |
        | 3  | event3 |
        | 4  | event4 |
      # Year transform creates event_time_year=YYYY directories
      Then data files in location count is at least 1
