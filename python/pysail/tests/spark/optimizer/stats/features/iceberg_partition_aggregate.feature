Feature: Iceberg Partition Column Aggregates

  Rule: MIN/MAX on integer partition column resolves from metadata
    Background:
      Given variable location for temporary directory ice_partition_agg_int
      Given final statement
        """
        DROP TABLE IF EXISTS ice_part_agg_int
        """
      Given statement template
        """
        CREATE TABLE ice_part_agg_int (
          id INT,
          value DOUBLE,
          year INT
        )
        USING iceberg
        PARTITIONED BY (year)
        LOCATION 'file://{{ location.string }}'
        """

    Scenario: MIN on Iceberg integer partition column
      Given statement
        """
        INSERT INTO ice_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year FROM ice_part_agg_int
        """
      Then query result
        | min_year |
        | 2023     |

    Scenario: MAX on Iceberg integer partition column
      Given statement
        """
        INSERT INTO ice_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MAX(year) AS max_year FROM ice_part_agg_int
        """
      Then query result
        | max_year |
        | 2025     |

    Scenario: MIN and MAX together on Iceberg integer partition column
      Given statement
        """
        INSERT INTO ice_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM ice_part_agg_int
        """
      Then query result
        | min_year | max_year |
        | 2023     | 2025     |

  Rule: EXPLAIN shows metadata-only plan for MIN/MAX on Iceberg partition columns
    Background:
      Given variable location for temporary directory ice_partition_agg_explain
      Given final statement
        """
        DROP TABLE IF EXISTS ice_part_agg_explain
        """
      Given statement template
        """
        CREATE TABLE ice_part_agg_explain (
          id INT,
          value DOUBLE,
          year INT
        )
        USING iceberg
        PARTITIONED BY (year)
        LOCATION 'file://{{ location.string }}'
        """

    Scenario: EXPLAIN MIN on Iceberg partition column shows no scan
      Given statement
        """
        INSERT INTO ice_part_agg_explain VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        EXPLAIN
        SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM ice_part_agg_explain
        """
      Then query plan matches snapshot

  Rule: COUNT on Iceberg partitioned table resolves from metadata
    Background:
      Given variable location for temporary directory ice_partition_agg_count
      Given final statement
        """
        DROP TABLE IF EXISTS ice_part_agg_count
        """
      Given statement template
        """
        CREATE TABLE ice_part_agg_count (
          id INT,
          year INT
        )
        USING iceberg
        PARTITIONED BY (year)
        LOCATION 'file://{{ location.string }}'
        """

    Scenario: COUNT resolves from metadata on Iceberg partitioned table
      Given statement
        """
        INSERT INTO ice_part_agg_count VALUES
          (1, 2023), (2, 2023),
          (3, 2024), (4, 2024),
          (5, 2025)
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM ice_part_agg_count
        """
      Then query result
        | cnt |
        | 5   |
