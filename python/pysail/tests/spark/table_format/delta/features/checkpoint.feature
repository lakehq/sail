Feature: Delta Lake Checkpoint

  @sail-only
  Rule: Checkpoint parquet and _last_checkpoint files are created at the configured interval

    Background:
      Given variable location for temporary directory delta_checkpoint_layout
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_layout_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_layout_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.checkpointInterval' = '1')
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_layout_test VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_layout_test VALUES (3)
        """

    Scenario: Delta log directory contains checkpoint parquet and _last_checkpoint after inserts
      When query
        """
        SELECT * FROM delta_checkpoint_layout_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
      Then delta log first commit protocol and metadata contains
        | path                                                | value |
        | metaData.configuration['delta.checkpointInterval'] | "1"   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.checkpoint.parquet
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: Multiple checkpoints are created after repeated writes with interval three

    Background:
      Given variable location for temporary directory delta_checkpoint_interval_three
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_interval_three_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_interval_three_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.checkpointInterval' = '3')
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (3)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (5)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (6)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_interval_three_test VALUES (7)
        """

    Scenario: Delta log directory contains two checkpoint parquet files after enough inserts
      When query
        """
        SELECT * FROM delta_checkpoint_interval_three_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |
        | 6  |
        | 7  |
      Then delta log first commit protocol and metadata contains
        | path                                                | value |
        | metaData.configuration['delta.checkpointInterval'] | "3"   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.checkpoint.parquet
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 00000000000000000005.crc
        📄 00000000000000000005.json
        📄 00000000000000000006.checkpoint.parquet
        📄 00000000000000000006.crc
        📄 00000000000000000006.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: Table remains readable from checkpoint when a JSON commit log is deleted

    Background:
      Given variable location for temporary directory delta_checkpoint_recovery
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_recovery_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_recovery_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.checkpointInterval' = '1')
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_recovery_test VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_recovery_test VALUES (3)
        """

    Scenario: Latest read succeeds after v1 JSON log is deleted
      Given file 00000000000000000001.json in delta_log is deleted
      When query
        """
        SELECT * FROM delta_checkpoint_recovery_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

  @sail-only
  Rule: Expired remove actions are pruned from checkpoint parquet based on table properties

    Background:
      Given variable location for temporary directory delta_checkpoint_deleted_file_retention
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_deleted_file_retention_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_deleted_file_retention_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.deletedFileRetentionDuration' = 'interval 0 seconds'
        )
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_deleted_file_retention_test VALUES (1)
        """
      Given statement
        """
        DELETE FROM delta_checkpoint_deleted_file_retention_test WHERE id = 1
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_deleted_file_retention_test VALUES (2)
        """

    Scenario: Checkpoint parquet omits expired remove actions
      When query
        """
        SELECT * FROM delta_checkpoint_deleted_file_retention_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
      Then delta log first commit protocol and metadata contains
        | path                                                          | value                |
        | metaData.configuration['delta.checkpointInterval']           | "2"                  |
        | metaData.configuration['delta.deletedFileRetentionDuration'] | "interval 0 seconds" |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.checkpoint.parquet
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: Expired Delta log files are cleaned up after checkpoint creation when enabled by table properties

    Background:
      Given variable location for temporary directory delta_checkpoint_log_cleanup
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_log_cleanup_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_log_cleanup_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.logRetentionDuration' = 'interval 0 seconds',
          'delta.enableExpiredLogCleanup' = 'true'
        )
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_test VALUES (3)
        """

    Scenario: Old JSON commit logs are physically deleted after a later checkpoint
      Then delta log first commit protocol and metadata contains
        | path                                                     | value                |
        | metaData.configuration['delta.checkpointInterval']      | "2"                  |
        | metaData.configuration['delta.logRetentionDuration']    | "interval 0 seconds" |
        | metaData.configuration['delta.enableExpiredLogCleanup'] | "true"               |
      Given delta log JSON files for versions 0, 1, 2 in delta_log are backdated by 172800 seconds
      Given sleep for 1 seconds
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_test VALUES (5)
        """
      When query
        """
        SELECT * FROM delta_checkpoint_log_cleanup_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |
      Then file tree in delta_log matches
        """
        📄 00000000000000000002.checkpoint.parquet
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.checkpoint.parquet
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: Expired Delta log cleanup honors in-commit timestamps

    Background:
      Given variable location for temporary directory delta_checkpoint_log_cleanup_ict
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_log_cleanup_ict_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_log_cleanup_ict_test
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.logRetentionDuration' = 'interval 0 seconds',
          'delta.enableExpiredLogCleanup' = 'true',
          'delta.enableInCommitTimestamps' = 'true'
        )
        AS SELECT 1 AS id
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_ict_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_ict_test VALUES (3)
        """
      Given delta log commit and checksum timestamps for versions 0, 1, 2 in delta_log are 100, 200, 300 milliseconds since epoch
      Given delta log JSON file timestamps for versions 0, 1, 2 in delta_log are 2147483646, 2147483646, 2147483646 seconds since epoch

    Scenario: Old JSON commit logs are cleaned up by in-commit timestamps instead of JSON mtimes
      Then delta log first commit protocol and metadata contains
        | path                                                      | value                |
        | metaData.configuration['delta.checkpointInterval']       | "2"                  |
        | metaData.configuration['delta.logRetentionDuration']     | "interval 0 seconds" |
        | metaData.configuration['delta.enableExpiredLogCleanup']  | "true"               |
        | metaData.configuration['delta.enableInCommitTimestamps'] | "true"               |
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_ict_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_log_cleanup_ict_test VALUES (5)
        """
      When query
        """
        SELECT * FROM delta_checkpoint_log_cleanup_ict_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |
      Then file tree in delta_log matches
        """
        📄 00000000000000000002.checkpoint.parquet
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.checkpoint.parquet
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: EXPLAIN shows checkpoint parquet in metadata-as-data log replay

    Background:
      Given variable location for temporary directory delta_checkpoint_explain
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_explain_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_explain_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES ('delta.checkpointInterval' = '1')
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_explain_test VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_explain_test VALUES (3)
        """

    Scenario: EXPLAIN SELECT references checkpoint parquet in log replay
      When query
        """
        EXPLAIN SELECT * FROM delta_checkpoint_explain_test ORDER BY id
        """
      Then query plan matches snapshot

  @sail-only
  Rule: EXPLAIN shows checkpoint parquet together with later JSON log commits

    Background:
      Given variable location for temporary directory delta_checkpoint_and_json_explain
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_and_json_explain_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_and_json_explain_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES ('delta.checkpointInterval' = '3')
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_and_json_explain_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_and_json_explain_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_and_json_explain_test VALUES (3)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_and_json_explain_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_and_json_explain_test VALUES (5)
        """

    Scenario: EXPLAIN SELECT references checkpoint parquet and later JSON commits in log replay
      When query
        """
        EXPLAIN SELECT * FROM delta_checkpoint_and_json_explain_test ORDER BY id
        """
      Then delta log first commit protocol and metadata contains
        | path                                                | value |
        | metaData.configuration['delta.checkpointInterval'] | "3"   |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.checkpoint.parquet
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 _last_checkpoint
        """
      Then query plan matches snapshot
