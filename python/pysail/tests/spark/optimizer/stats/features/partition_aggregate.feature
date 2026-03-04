Feature: Delta Lake Partition Column Aggregates

  Rule: MIN/MAX on integer partition column resolves from metadata
    Background:
      Given variable location for temporary directory delta_partition_agg_int
      Given final statement
        """
        DROP TABLE IF EXISTS delta_part_agg_int
        """
      Given statement template
        """
        CREATE TABLE delta_part_agg_int (
          id INT,
          value DOUBLE,
          year INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: MIN on integer partition column
      Given statement
        """
        INSERT INTO delta_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year FROM delta_part_agg_int
        """
      Then query result
        | min_year |
        | 2023     |

    Scenario: MAX on integer partition column
      Given statement
        """
        INSERT INTO delta_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MAX(year) AS max_year FROM delta_part_agg_int
        """
      Then query result
        | max_year |
        | 2025     |

    Scenario: MIN and MAX together on integer partition column
      Given statement
        """
        INSERT INTO delta_part_agg_int VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM delta_part_agg_int
        """
      Then query result
        | min_year | max_year |
        | 2023     | 2025     |

  Rule: EXPLAIN shows metadata-only plan for MIN/MAX on partition columns
    Background:
      Given variable location for temporary directory delta_partition_agg_explain
      Given final statement
        """
        DROP TABLE IF EXISTS delta_part_agg_explain
        """
      Given statement template
        """
        CREATE TABLE delta_part_agg_explain (
          id INT,
          value DOUBLE,
          year INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: EXPLAIN MIN on partition column shows no scan
      Given statement
        """
        INSERT INTO delta_part_agg_explain VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        EXPLAIN
        SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM delta_part_agg_explain
        """
      Then query plan matches snapshot

  Rule: MIN/MAX on multi-column partitioning
    Background:
      Given variable location for temporary directory delta_partition_agg_multi
      Given final statement
        """
        DROP TABLE IF EXISTS delta_part_agg_multi
        """
      Given statement template
        """
        CREATE TABLE delta_part_agg_multi (
          id INT,
          region INT,
          category INT
        )
        USING DELTA
        PARTITIONED BY (region, category)
        LOCATION {{ location.sql }}
        """

    Scenario: MIN/MAX on each partition column independently
      Given statement
        """
        INSERT INTO delta_part_agg_multi VALUES
          (1, 10, 100),
          (2, 20, 200),
          (3, 30, 300)
        """
      When query
        """
        SELECT
          MIN(region) AS min_r, MAX(region) AS max_r,
          MIN(category) AS min_c, MAX(category) AS max_c
        FROM delta_part_agg_multi
        """
      Then query result
        | min_r | max_r | min_c | max_c |
        | 10    | 30    | 100   | 300   |

  Rule: COUNT on partitioned table still works
    Background:
      Given variable location for temporary directory delta_partition_agg_count
      Given final statement
        """
        DROP TABLE IF EXISTS delta_part_agg_count
        """
      Given statement template
        """
        CREATE TABLE delta_part_agg_count (
          id INT,
          year INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: COUNT still resolves from metadata on partitioned table
      Given statement
        """
        INSERT INTO delta_part_agg_count VALUES
          (1, 2023), (2, 2023),
          (3, 2024), (4, 2024),
          (5, 2025)
        """
      When query
        """
        SELECT COUNT(*) AS cnt FROM delta_part_agg_count
        """
      Then query result
        | cnt |
        | 5   |

  Rule: MIN/MAX with filter falls back to scan
    Background:
      Given variable location for temporary directory delta_partition_agg_filter
      Given final statement
        """
        DROP TABLE IF EXISTS delta_part_agg_filter
        """
      Given statement template
        """
        CREATE TABLE delta_part_agg_filter (
          id INT,
          value DOUBLE,
          year INT
        )
        USING DELTA
        PARTITIONED BY (year)
        LOCATION {{ location.sql }}
        """

    Scenario: MIN with filter still returns correct result
      Given statement
        """
        INSERT INTO delta_part_agg_filter VALUES
          (1, 1.0, 2023),
          (2, 2.0, 2024),
          (3, 3.0, 2025)
        """
      When query
        """
        SELECT MIN(year) AS min_year FROM delta_part_agg_filter WHERE value > 1.5
        """
      Then query result
        | min_year |
        | 2024     |
