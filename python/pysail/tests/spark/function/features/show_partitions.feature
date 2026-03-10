Feature: SHOW PARTITIONS

  Rule: Non-partitioned tables

    Scenario: SHOW PARTITIONS on non-partitioned table raises AnalysisException
      Given statement
        """
        DROP TABLE IF EXISTS show_part_np
        """
      Given statement
        """
        CREATE TABLE show_part_np (id INT, name STRING) USING parquet
        """
      Given final statement
        """
        DROP TABLE IF EXISTS show_part_np
        """
      When query
        """
        SHOW PARTITIONS show_part_np
        """
      Then query error PARTITION_SCHEMA_IS_EMPTY

  Rule: Partitioned tables

    Scenario: SHOW PARTITIONS on empty partitioned table
      Given statement
        """
        DROP TABLE IF EXISTS show_part_empty
        """
      Given statement
        """
        CREATE TABLE show_part_empty (id INT, name STRING, year INT) USING parquet PARTITIONED BY (year)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS show_part_empty
        """
      When query
        """
        SHOW PARTITIONS show_part_empty
        """
      Then query result
        | partition |

    Scenario: SHOW PARTITIONS lists single-column partition values
      Given statement
        """
        DROP TABLE IF EXISTS show_part_single
        """
      Given statement
        """
        CREATE TABLE show_part_single (id INT, name STRING, year INT) USING parquet PARTITIONED BY (year)
        """
      Given statement
        """
        INSERT INTO show_part_single VALUES (1, 'a', 2023), (2, 'b', 2024), (3, 'c', 2023)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS show_part_single
        """
      When query
        """
        SHOW PARTITIONS show_part_single
        """
      Then query result
        | partition |
        | year=2023 |
        | year=2024 |

    @sail-only
    Scenario: SHOW PARTITIONS with PARTITION filter clause is not supported
      Given statement
        """
        DROP TABLE IF EXISTS show_part_filter
        """
      Given statement
        """
        CREATE TABLE show_part_filter (id INT, year INT, month INT) USING parquet PARTITIONED BY (year, month)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS show_part_filter
        """
      When query
        """
        SHOW PARTITIONS show_part_filter PARTITION (year=2023)
        """
      Then query error not supported

    Scenario: SHOW PARTITIONS lists multi-column partition values
      Given statement
        """
        DROP TABLE IF EXISTS show_part_multi
        """
      Given statement
        """
        CREATE TABLE show_part_multi (id INT, year INT, month INT) USING parquet PARTITIONED BY (year, month)
        """
      Given statement
        """
        INSERT INTO show_part_multi VALUES (1, 2023, 1), (2, 2023, 2), (3, 2024, 1)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS show_part_multi
        """
      When query
        """
        SHOW PARTITIONS show_part_multi
        """
      Then query result
        | partition         |
        | year=2023/month=1 |
        | year=2023/month=2 |
        | year=2024/month=1 |
