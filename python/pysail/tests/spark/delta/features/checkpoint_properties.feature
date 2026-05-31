Feature: Delta Lake checkpoint properties

  Rule: Delta checkpoint interval properties materialize into first-commit metadata
    Background:
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_properties_ddl
        """
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_properties_ctas
        """

    Scenario: CREATE TABLE with Delta checkpoint interval property persists metadata on first write
      Given variable location for temporary directory delta_checkpoint_properties_ddl
      Given statement template
        """
        CREATE TABLE delta_checkpoint_properties_ddl (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1'
        )
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_properties_ddl VALUES (1), (2)
        """
      When query
        """
        SELECT * FROM delta_checkpoint_properties_ddl ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
      Then delta log first commit protocol and metadata contains
        | path                                                   | value |
        | metaData.configuration['delta.checkpointInterval']     | "1"   |

    Scenario: CTAS with Delta checkpoint interval property succeeds and persists metadata
      Given variable location for temporary directory delta_checkpoint_properties_ctas
      Given statement template
        """
        CREATE TABLE delta_checkpoint_properties_ctas
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '3'
        )
        AS SELECT * FROM VALUES
          (1),
          (2)
        AS t(id)
        """
      When query
        """
        SELECT * FROM delta_checkpoint_properties_ctas ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
      Then delta log first commit protocol and metadata contains
        | path                                                   | value |
        | metaData.configuration['delta.checkpointInterval']     | "3"   |

  @sail-only
  Rule: delta.checkpointPolicy controls whether V2 or classic checkpoints are written

    Scenario: Setting delta.checkpointPolicy to classic writes classic checkpoint without sidecar files
      Given variable location for temporary directory delta_checkpoint_policy_classic
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_checkpoint_policy_classic_test
        """
      Given statement template
        """
        CREATE TABLE delta_checkpoint_policy_classic_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '1',
          'delta.checkpointPolicy' = 'classic'
        )
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_policy_classic_test VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_checkpoint_policy_classic_test VALUES (2)
        """
      When query
        """
        SELECT * FROM delta_checkpoint_policy_classic_test ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.checkpoint.parquet
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 _last_checkpoint
        """
