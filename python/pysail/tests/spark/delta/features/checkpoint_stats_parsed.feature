Feature: Delta Lake Checkpoint stats_parsed and partitionValues_parsed

  @sail-only
  Rule: Checkpoint parquet does not emit stats_parsed when writeStatsAsStruct is unset

    Background:
      Given variable location for temporary directory delta_cp_stats_default
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_default_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_default_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.checkpointInterval' = '1')
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_default_test VALUES (1), (2), (3)
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_default_test VALUES (4)
        """

    Scenario: Checkpoint has no stats_parsed or partitionValues_parsed sub-field
      When query
        """
        SELECT * FROM delta_cp_stats_default_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location does not contain add sub-field stats_parsed
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location does not contain add sub-field partitionValues_parsed

  @sail-only
  Rule: Checkpoint emits stats_parsed when writeStatsAsStruct is true on an unpartitioned table

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_unpart
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_struct_unpart_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_struct_unpart_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpoint.writeStatsAsStruct' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_unpart_test VALUES (7), (42), (100)
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_unpart_test VALUES (200)
        """

    Scenario: stats_parsed struct column holds typed minValues / maxValues / numRecords
      When query
        """
        SELECT * FROM delta_cp_stats_struct_unpart_test ORDER BY id
        """
      Then query result ordered
        | id  |
        | 7   |
        | 42  |
        | 100 |
        | 200 |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains add fields
        | path                     | value |
        | stats_parsed.numRecords  | 3     |
        | stats_parsed.minValues.id | 7     |
        | stats_parsed.maxValues.id | 100   |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location does not contain add sub-field partitionValues_parsed

  @sail-only
  Rule: Checkpoint emits partitionValues_parsed when writeStatsAsStruct is true on a partitioned table

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_part
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_struct_part_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_struct_part_test (id INT, year INT, region STRING)
        USING DELTA
        PARTITIONED BY (year, region)
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpoint.writeStatsAsStruct' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_part_test VALUES (1, 2024, 'us'), (2, 2024, 'us'), (3, 2024, 'us')
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_part_test VALUES (4, 2024, 'us')
        """

    Scenario: partitionValues_parsed carries typed partition values
      When query
        """
        SELECT id, year, region FROM delta_cp_stats_struct_part_test ORDER BY id
        """
      Then query result ordered
        | id | year | region |
        | 1  | 2024 | us     |
        | 2  | 2024 | us     |
        | 3  | 2024 | us     |
        | 4  | 2024 | us     |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains add fields
        | path                              | value  |
        | stats_parsed.numRecords           | 3      |
        | stats_parsed.minValues.id         | 1      |
        | stats_parsed.maxValues.id         | 3      |
        | partitionValues_parsed.year       | 2024   |
        | partitionValues_parsed.region     | "us"   |

  @sail-only
  Rule: Metadata-as-data and snapshot-materialized read paths produce identical pruned results

    Background:
      Given variable location for temporary directory delta_cp_stats_read_parity
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_read_parity_struct
        """
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_read_parity_json
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_read_parity_struct (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpoint.writeStatsAsStruct' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_read_parity_struct VALUES (1), (5), (10), (50), (100)
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_read_parity_json
        USING DELTA
        LOCATION {{ location.sql }}
        """

    Scenario: Pruning predicate returns equivalent rows on both read paths
      When query
        """
        SELECT id FROM delta_cp_stats_read_parity_struct WHERE id >= 10 ORDER BY id
        """
      Then query result ordered
        | id  |
        | 10  |
        | 50  |
        | 100 |
