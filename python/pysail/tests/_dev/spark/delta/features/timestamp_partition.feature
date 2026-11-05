Feature: Delta Lake timestamp-partitioned tables

  Rule: Writes and reads timestamp-partitioned tables correctly

    Background:
      Given variable location for temporary directory delta_ts_partition
      Given config spark.sql.session.timeZone = UTC
      Given final statement
        """
        DROP TABLE IF EXISTS delta_ts_part_test
        """
      Given statement template
        """
        CREATE TABLE delta_ts_part_test (
          id INT,
          ts TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (ts)
        LOCATION {{ location.sql }}
        """

    Scenario: Timestamp-partitioned table has percent-encoded partition directories
      Given statement
        """
        INSERT INTO delta_ts_part_test VALUES
          (1, TIMESTAMP '2024-01-15 10:30:00'),
          (2, TIMESTAMP '2024-06-20 15:45:00.123456')
        """
      Then file tree in location matches
        """
        📂 ts=2024-01-15%2010%3A30%3A00.000000
          📄 part-<id>.<codec>.parquet
        📂 ts=2024-06-20%2015%3A45%3A00.123456
          📄 part-<id>.<codec>.parquet
        """

    Scenario: Read back rows from a timestamp-partitioned table it wrote
      Given statement
        """
        INSERT INTO delta_ts_part_test VALUES
          (1, TIMESTAMP '2024-01-15 10:30:00'),
          (2, TIMESTAMP '2024-06-20 15:45:00.123456')
        """
      When query
        """
        SELECT id, CAST(ts AS STRING) AS ts_str
        FROM delta_ts_part_test
        ORDER BY id
        """
      Then query result
        | id | ts_str                     |
        | 1  | 2024-01-15 10:30:00        |
        | 2  | 2024-06-20 15:45:00.123456 |


  Rule: Row operations resolve URI-encoded file paths
    Background:
      Given config spark.sql.session.timeZone = UTC
      Given variable location for temporary directory delta_encoded_row_paths
      Given final statement
        """
        DROP TABLE IF EXISTS delta_encoded_row_paths
        """
      Given statement template
        """
        CREATE TABLE delta_encoded_row_paths (id INT, value STRING, part <partition_type>)
        USING DELTA
        PARTITIONED BY (part)
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableDeletionVectors' = '<dv>')
        """
      Given statement
        """
        INSERT INTO delta_encoded_row_paths VALUES
          (1, 'old', <first_partition>),
          (2, 'keep', <first_partition>),
          (3, 'keep', <first_partition>),
          (4, 'keep', <first_partition>)
        """
      Given statement
        """
        INSERT INTO delta_encoded_row_paths VALUES
          (5, 'keep', <first_partition>),
          (6, 'keep', <first_partition>)
        """
      Given statement
        """
        INSERT INTO delta_encoded_row_paths VALUES
          (7, 'keep', <second_partition>),
          (8, 'keep', <second_partition>)
        """
      Then data files in location count is 3

    Scenario Outline: Row operations preserve files with URI-encoded paths
      Given statement
        """
        <statement>
        """
      Then delta log latest removes in location match adds from versions <removed_versions>
      When query
        """
        SELECT id, value FROM delta_encoded_row_paths ORDER BY id
        """
      Then query result ordered
        | id | value    |
        | 1  | new      |
        | 2  | keep     |
        | 3  | keep     |
        | 4  | keep     |
        | 5  | keep     |
        | 6  | keep     |
        | 7  | <value7> |
        | 8  | keep     |

      Examples:
        | case                  | dv    | partition_type | first_partition                 | second_partition                | removed_versions | value7 | statement                                                                                                                                                                                                                           |
        | timestamp-cow-merge   | false | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1                | keep   | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                               |
        | timestamp-cow-pruned  | false | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1                | keep   | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id = s.id AND t.part = TIMESTAMP '2026-09-10 12:34:56' WHEN MATCHED THEN UPDATE SET value = 'new'                                                                   |
        | timestamp-cow-many    | false | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1, 3             | new    | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id % 6 = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                           |
        | timestamp-cow-update  | false | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1                | keep   | UPDATE delta_encoded_row_paths SET value = 'new' WHERE id = 1                                                                                                                                                                       |
        | timestamp-mor-merge   | true  | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1                | keep   | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                               |
        | timestamp-mor-many    | true  | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1, 3             | new    | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id % 6 = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                           |
        | timestamp-mor-update  | true  | TIMESTAMP      | TIMESTAMP '2026-09-10 12:34:56' | TIMESTAMP '2026-09-11 12:34:56' | 1                | keep   | UPDATE delta_encoded_row_paths SET value = 'new' WHERE id = 1                                                                                                                                                                       |
        | escaped-string-cow    | false | STRING         | 'a +%2F 中'                     | 'other'                         | 1                | keep   | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                               |
        | escaped-string-mor    | true  | STRING         | 'a +%2F 中'                     | 'other'                         | 1                | keep   | MERGE INTO delta_encoded_row_paths t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = 'new'                                                                                                               |
