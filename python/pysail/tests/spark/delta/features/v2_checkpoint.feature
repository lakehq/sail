Feature: Delta Lake V2 Checkpoint (Sidecar Checkpoints)

  @sail-only
  Rule: V2 checkpoint with sidecars is created when v2Checkpoint table feature is enabled

    Background:
      Given variable location for temporary directory delta_v2_checkpoint
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_checkpoint_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_checkpoint_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_test VALUES (1, 'a')
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_test VALUES (2, 'b')
        """

    Scenario: V2 checkpoint creates UUID-named checkpoint and sidecar files
      When query
        """
        SELECT * FROM delta_v2_checkpoint_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | a     |
        | 2  | b     |
      Then file tree in delta_log matches
        """
        📂 _sidecars
          📄 <uuid>.parquet
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.checkpoint.<uuid>.parquet
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: Table is readable from V2 checkpoint after commit JSON is deleted

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_recovery
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_checkpoint_recovery_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_checkpoint_recovery_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_recovery_test VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_recovery_test VALUES (3)
        """

    Scenario: Latest read succeeds after v1 JSON log is deleted with V2 checkpoint present
      Given file 00000000000000000001.json in delta_log is deleted
      When query
        """
        SELECT * FROM delta_v2_checkpoint_recovery_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

  @sail-only
  Rule: Multiple V2 checkpoints are created after repeated writes

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_multi
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_checkpoint_multi_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_checkpoint_multi_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_multi_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_multi_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_multi_test VALUES (3)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_multi_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_multi_test VALUES (5)
        """

    Scenario: Delta log directory contains two V2 checkpoints with sidecar files
      When query
        """
        SELECT * FROM delta_v2_checkpoint_multi_test ORDER BY id
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
        📂 _sidecars
          📄 <uuid>.parquet
          📄 <uuid>.parquet
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.checkpoint.<uuid>.parquet
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.checkpoint.<uuid>.parquet
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: V2 checkpoint is activated by delta.checkpointPolicy = v2

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_policy
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_checkpoint_policy_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_checkpoint_policy_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpointPolicy' = 'v2'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_policy_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_policy_test VALUES (2)
        """

    Scenario: delta.checkpointPolicy = v2 produces UUID-named checkpoint and sidecar files
      When query
        """
        SELECT * FROM delta_v2_checkpoint_policy_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
      Then file tree in delta_log matches
        """
        📂 _sidecars
          📄 <uuid>.parquet
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.checkpoint.<uuid>.parquet
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 _last_checkpoint
        """

  @sail-only
  Rule: V2 checkpoint correctly replays state with deletes

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_delete
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_checkpoint_delete_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_checkpoint_delete_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_delete_test VALUES (1), (2), (3)
        """
      Given statement
        """
        DELETE FROM delta_v2_checkpoint_delete_test WHERE id = 2
        """
      Given statement
        """
        INSERT INTO delta_v2_checkpoint_delete_test VALUES (4)
        """

    Scenario: Table state after delete and checkpoint-based replay is correct
      When query
        """
        SELECT * FROM delta_v2_checkpoint_delete_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |
        | 4  |

  @sail-only
  Rule: EXPLAIN shows driver path reading V2 checkpoint

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_explain_driver
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_ckpt_explain_driver_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_ckpt_explain_driver_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_explain_driver_test VALUES (1, 'a')
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_explain_driver_test VALUES (2, 'b')
        """

    Scenario: EXPLAIN SELECT on V2 checkpoint table uses driver file scan
      When query
        """
        EXPLAIN SELECT * FROM delta_v2_ckpt_explain_driver_test ORDER BY id
        """
      Then query plan matches snapshot

  @sail-only
  Rule: EXPLAIN shows metadata-as-data path reading V2 checkpoint with sidecar log replay

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_explain_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_ckpt_explain_metadata_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_ckpt_explain_metadata_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_explain_metadata_test VALUES (1, 'a')
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_explain_metadata_test VALUES (2, 'b')
        """

    Scenario: EXPLAIN SELECT on V2 checkpoint table with metadata-as-data shows sidecar in log replay
      When query
        """
        EXPLAIN SELECT * FROM delta_v2_ckpt_explain_metadata_test ORDER BY id
        """
      Then query plan matches snapshot

  @sail-only
  Rule: V2 checkpoint table is readable via metadata-as-data path

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_metadata_read
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_ckpt_metadata_read_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_ckpt_metadata_read_test (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_metadata_read_test VALUES (1, 'a')
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_metadata_read_test VALUES (2, 'b')
        """

    Scenario: SELECT on V2 checkpoint table with metadata-as-data returns correct data
      When query
        """
        SELECT * FROM delta_v2_ckpt_metadata_read_test ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | a     |
        | 2  | b     |

  @sail-only
  Rule: V2 checkpoint table with metadata-as-data is readable after JSON log deletion

    Background:
      Given variable location for temporary directory delta_v2_checkpoint_metadata_recovery
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_ckpt_metadata_recovery_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_ckpt_metadata_recovery_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        OPTIONS (metadataAsDataRead 'true')
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_metadata_recovery_test VALUES (1), (2)
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_metadata_recovery_test VALUES (3)
        """

    Scenario: metadata-as-data read succeeds after v1 JSON log is deleted with V2 checkpoint
      Given file 00000000000000000001.json in delta_log is deleted
      When query
        """
        SELECT * FROM delta_v2_ckpt_metadata_recovery_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

  @sail-only
  Rule: Log cleanup writes a classic compat checkpoint before deleting V2 checkpoint era logs

    Background:
      Given variable location for temporary directory delta_v2_ckpt_log_cleanup
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_v2_ckpt_log_cleanup_test
        """
      Given statement template
        """
        CREATE TABLE delta_v2_ckpt_log_cleanup_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '2',
          'delta.logRetentionDuration' = 'interval 0 seconds',
          'delta.enableExpiredLogCleanup' = 'true',
          'delta.feature.v2Checkpoint' = 'enabled'
        )
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_log_cleanup_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_log_cleanup_test VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_log_cleanup_test VALUES (3)
        """

    Scenario: Classic compat checkpoint is created at retention boundary before log files are removed
      Given delta log JSON files for versions 0, 1, 2 in delta_log are backdated by 172800 seconds
      Given sleep for 1 seconds
      Given statement
        """
        INSERT INTO delta_v2_ckpt_log_cleanup_test VALUES (4)
        """
      Given statement
        """
        INSERT INTO delta_v2_ckpt_log_cleanup_test VALUES (5)
        """
      When query
        """
        SELECT * FROM delta_v2_ckpt_log_cleanup_test ORDER BY id
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
        📂 _sidecars
          📄 <uuid>.parquet
          📄 <uuid>.parquet
        📄 00000000000000000002.checkpoint.<uuid>.parquet
        📄 00000000000000000002.checkpoint.parquet
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 00000000000000000004.checkpoint.<uuid>.parquet
        📄 00000000000000000004.crc
        📄 00000000000000000004.json
        📄 _last_checkpoint
        """
