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

  Rule: Truncate transform partitioning
    Background:
      Given variable location for temporary directory iceberg_part_truncate
      Given final statement
        """
        DROP TABLE IF EXISTS part_truncate
        """

  Rule: Year/Month/Day transforms on timestamp
    Background:
      Given variable location for temporary directory iceberg_part_temporal
      Given final statement
        """
        DROP TABLE IF EXISTS part_temporal
        """
