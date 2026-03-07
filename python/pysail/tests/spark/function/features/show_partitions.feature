Feature: SHOW PARTITIONS

  Rule: Non-partitioned tables

    Scenario: SHOW PARTITIONS on non-partitioned table raises AnalysisException
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW np_view AS SELECT 1 AS id
        """
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

    # TODO: listing actual partition values is not yet implemented.
    #   These tests only verify schema and error handling.
    #   Once partition directory listing is implemented, add tests
    #   that INSERT data and verify SHOW PARTITIONS returns partition values.

    Scenario: SHOW PARTITIONS on empty partitioned table returns empty result with correct schema
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
