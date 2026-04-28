Feature: Delta Lake Checkpoint stats_parsed and partitionValues_parsed

  @sail-only
  Rule: Checkpoint parquet emits parsed stats fields by default

    Background:
      Given variable location for temporary directory delta_cp_stats_default
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

    Scenario: Checkpoint has stats_parsed but no partitionValues_parsed sub-field
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
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains add fields
        | path                     | value |
        | stats_parsed.numRecords  | 3     |
        | stats_parsed.minValues.id | 1     |
        | stats_parsed.maxValues.id | 3     |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location does not contain add sub-field partitionValues_parsed

  @sail-only
  Rule: Checkpoint parsed stats fields can be disabled

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_disabled
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_struct_disabled_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_struct_disabled_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpoint.writeStatsAsStruct' = 'false'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_disabled_test VALUES (1), (2), (3)
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_disabled_test VALUES (4)
        """

    Scenario: Checkpoint has no stats_parsed or partitionValues_parsed sub-field when disabled
      When query
        """
        SELECT * FROM delta_cp_stats_struct_disabled_test ORDER BY id
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
  Rule: Checkpoint emits stats_parsed when writeStatsAsStruct is true

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_unpart
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

    Scenario: stats_parsed struct column holds typed statistics
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
  Rule: Checkpoint emits partitionValues_parsed for partitioned struct-stats tables

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_part
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
        | path                          | value  |
        | stats_parsed.numRecords       | 3      |
        | stats_parsed.minValues.id     | 1      |
        | stats_parsed.maxValues.id     | 3      |
        | partitionValues_parsed.year   | 2024   |
        | partitionValues_parsed.region | "us"   |

  @sail-only
  Rule: Column-mapped parsed checkpoint fields use physical names

    Background:
      Given variable location for temporary directory delta_cp_stats_cm_part
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_cm_part_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_cm_part_test (id INT, data STRING, region STRING)
        USING DELTA
        PARTITIONED BY (region)
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_cm_part_test VALUES (1, 'a', 'us'), (2, 'b', 'us')
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_cm_part_test VALUES (3, 'c', 'us')
        """

    Scenario: stats_parsed and partitionValues_parsed are keyed by physical names
      When query
        """
        SELECT id, data, region FROM delta_cp_stats_cm_part_test ORDER BY id
        """
      Then query result ordered
        | id | data | region |
        | 1  | a    | us     |
        | 2  | b    | us     |
        | 3  | c    | us     |
      Then delta log add partitionValues in location uses physical name for column region
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains physical stats_parsed minValues for column id with value 1
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains physical stats_parsed maxValues for column id with value 2
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains physical partitionValues_parsed for column region with value "us"

  @sail-only
  Rule: Checkpoint stats JSON is omitted when writeStatsAsJson is false

    Background:
      Given variable location for temporary directory delta_cp_stats_struct_only
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cp_stats_struct_only_test
        """
      Given statement template
        """
        CREATE TABLE delta_cp_stats_struct_only_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpoint.writeStatsAsStruct' = 'true',
          'delta.checkpoint.writeStatsAsJson' = 'false'
        )
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_only_test VALUES (5), (6), (7)
        """
      Given statement
        """
        INSERT INTO delta_cp_stats_struct_only_test VALUES (8)
        """

    Scenario: stats_parsed remains readable without the stats JSON sub-field
      When query
        """
        SELECT * FROM delta_cp_stats_struct_only_test WHERE id >= 6 ORDER BY id
        """
      Then query result ordered
        | id |
        | 6  |
        | 7  |
        | 8  |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location contains add fields
        | path                     | value |
        | stats_parsed.numRecords  | 3     |
        | stats_parsed.minValues.id | 5     |
        | stats_parsed.maxValues.id | 7     |
      Then checkpoint parquet file 00000000000000000001.checkpoint.parquet in location does not contain add sub-field stats

  @sail-only
  Rule: Metadata-as-data and snapshot-materialized read paths produce matching results

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
      When query
        """
        SELECT id FROM delta_cp_stats_read_parity_json WHERE id >= 10 ORDER BY id
        """
      Then query result ordered
        | id  |
        | 10  |
        | 50  |
        | 100 |
