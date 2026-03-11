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
        📄 00000000000000000000.json
        📄 00000000000000000001.checkpoint.parquet
        📄 00000000000000000001.json
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
