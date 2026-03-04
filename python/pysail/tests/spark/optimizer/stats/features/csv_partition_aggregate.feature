Feature: CSV Partition Column Aggregates

  Rule: MIN/MAX on CSV partition column
    Background:
      Given variable location for temporary directory csv_partition_agg_int
      Given final statement
        """
        DROP TABLE IF EXISTS csv_part_agg_int
        """
      Given statement template
        """
        CREATE TABLE csv_part_agg_int (
          id INT,
          value DOUBLE,
          year INT
        )
        USING CSV
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: MIN on CSV integer partition column
      Given statement
        """
        INSERT INTO csv_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year FROM csv_part_agg_int
        """
      Then query result
        | min_year |
        | 2023     |

    Scenario: MAX on CSV integer partition column
      Given statement
        """
        INSERT INTO csv_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MAX(year) AS max_year FROM csv_part_agg_int
        """
      Then query result
        | max_year |
        | 2025     |

  # Note: CSV has no column statistics (no file footer metadata like Parquet).
  # AggregateStatistics optimization does not apply — always does a full scan.
