Feature: Delta Lake Compacted JSON (Log Compaction)

  @sail-only
  Rule: Automatic log compaction creates compacted JSON files at the configured interval

    Background:
      Given variable location for temporary directory delta_compacted_json_auto
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_compacted_json_auto
        """
      Given statement template
        """
        CREATE TABLE delta_compacted_json_auto (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.logCompactionInterval' = '3')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_auto VALUES (1, 'one')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_auto VALUES (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_auto VALUES (3, 'three')
        """

    Scenario: Delta log contains auto-generated compacted JSON file after reaching interval
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.00000000000000000002.compacted.json
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        """

    Scenario: Reads all data correctly with auto-compacted JSON on driver path
      When query
        """
        SELECT id, value FROM delta_compacted_json_auto ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |

  @sail-only
  Rule: Metadata-as-data read path works correctly with auto-compacted JSON

    Background:
      Given variable location for temporary directory delta_compacted_json_mad
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_compacted_json_mad
        """
      Given statement template
        """
        CREATE TABLE delta_compacted_json_mad (id INT, value STRING)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.logCompactionInterval' = '3')
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_mad VALUES (1, 'one')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_mad VALUES (2, 'two')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_mad VALUES (3, 'three')
        """

    Scenario: Reads all data correctly with auto-compacted JSON on metadata-as-data path
      When query
        """
        SELECT id, value FROM delta_compacted_json_mad ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | one   |
        | 2  | two   |
        | 3  | three |

  @sail-only
  Rule: Compaction and checkpoint coexist correctly

    Background:
      Given variable location for temporary directory delta_compacted_json_cp
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_compacted_json_cp
        """
      Given statement template
        """
        CREATE TABLE delta_compacted_json_cp (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.checkpointInterval' = '3',
          'delta.logCompactionInterval' = '3'
        )
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_cp VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_cp VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_cp VALUES (3)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_cp VALUES (4)
        """

    Scenario: Checkpoint and compacted JSON both appear in the delta log
      Then file tree in delta_log matches
        """
        📄 00000000000000000000.00000000000000000002.compacted.json
        📄 00000000000000000000.crc
        📄 00000000000000000000.json
        📄 00000000000000000001.crc
        📄 00000000000000000001.json
        📄 00000000000000000002.crc
        📄 00000000000000000002.json
        📄 00000000000000000003.checkpoint.parquet
        📄 00000000000000000003.crc
        📄 00000000000000000003.json
        📄 _last_checkpoint
        """

    Scenario: Reads data correctly when both checkpoint and compaction are present
      When query
        """
        SELECT id FROM delta_compacted_json_cp ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |

  @sail-only
  Rule: Table is still readable after individual commits covered by compaction are deleted

    Background:
      Given variable location for temporary directory delta_compacted_json_deleted
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_compacted_json_deleted
        """
      Given statement template
        """
        CREATE TABLE delta_compacted_json_deleted (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.logCompactionInterval' = '3')
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_deleted VALUES (1)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_deleted VALUES (2)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_deleted VALUES (3)
        """
      Given statement
        """
        INSERT INTO delta_compacted_json_deleted VALUES (4)
        """

    Scenario: Reads data correctly after compacted commits are deleted
      Given file 00000000000000000000.json in delta_log is deleted
      Given file 00000000000000000001.json in delta_log is deleted
      Given file 00000000000000000002.json in delta_log is deleted
      When query template
        """
        SELECT id FROM delta.`{{ location.string }}` ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
