Feature: Delta Lake append-only tables

  Rule: delta.appendOnly controls protocol metadata and row removal

    Scenario: CREATE with append-only enabled permits inserts and rejects deletes
      Given variable location for temporary directory delta_append_only_create
      Given final statement
        """
        DROP TABLE IF EXISTS delta_append_only_create_test
        """
      Given statement template
        """
        CREATE TABLE delta_append_only_create_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.appendOnly' = 'true')
        """
      Then delta log first commit protocol and metadata contains
        | path                                                | value  |
        | protocol.minReaderVersion                           | 1      |
        | protocol.minWriterVersion                           | 2      |
        | metaData.configuration['delta.appendOnly']          | "true" |
      Given statement
        """
        INSERT INTO delta_append_only_create_test VALUES (1)
        """
      Given statement with error Delta table is append-only
        """
        DELETE FROM delta_append_only_create_test WHERE id = 1
        """
      When query
        """
        SELECT id FROM delta_append_only_create_test
        """
      Then query result
        | id |
        | 1  |

    Scenario: ALTER enables append-only and blocks deletes
      Given variable location for temporary directory delta_append_only_alter
      Given final statement
        """
        DROP TABLE IF EXISTS delta_append_only_alter_test
        """
      Given statement template
        """
        CREATE TABLE delta_append_only_alter_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_append_only_alter_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_append_only_alter_test
        SET TBLPROPERTIES ('delta.appendOnly' = 'true')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                | value  |
        | protocol.minReaderVersion                           | 1      |
        | protocol.minWriterVersion                           | 2      |
        | metaData.configuration['delta.appendOnly']          | "true" |
      Given statement with error Delta table is append-only
        """
        DELETE FROM delta_append_only_alter_test WHERE id = 1
        """
      When query
        """
        SELECT id FROM delta_append_only_alter_test
        """
      Then query result
        | id |
        | 1  |

    Scenario: UNSET disables append-only without downgrading the protocol
      Given variable location for temporary directory delta_append_only_unset
      Given final statement
        """
        DROP TABLE IF EXISTS delta_append_only_unset_test
        """
      Given statement template
        """
        CREATE TABLE delta_append_only_unset_test (id INT)
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_append_only_unset_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_append_only_unset_test
        SET TBLPROPERTIES ('delta.appendOnly' = 'true')
        """
      Given statement
        """
        ALTER TABLE delta_append_only_unset_test
        UNSET TBLPROPERTIES ('delta.appendOnly')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                | value |
        | protocol.minReaderVersion                           | 1     |
        | protocol.minWriterVersion                           | 2     |
      Given statement
        """
        DELETE FROM delta_append_only_unset_test WHERE id = 1
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_append_only_unset_test
        """
      Then query result
        | count |
        | 0     |
