Feature: Delta Lake ALTER TABLE SET/UNSET TBLPROPERTIES

  @sail-only
  Rule: ALTER TABLE SET TBLPROPERTIES persists properties to the Delta log

    Scenario: ALTER TABLE SET TBLPROPERTIES writes a new metadata action to the Delta log
      Given variable location for temporary directory delta_alter_table_set_props
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_set_props_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_set_props_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_alter_table_set_props_test VALUES (1, 'v0')
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_set_props_test
        SET TBLPROPERTIES ('delta.checkpointInterval' = '5', 'custom.key' = 'hello')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                        | value   |
        | metaData.configuration['delta.checkpointInterval']          | "5"     |
        | metaData.configuration['custom.key']                        | "hello" |

    Scenario: ALTER TABLE UNSET TBLPROPERTIES removes a property from the Delta log
      Given variable location for temporary directory delta_alter_table_unset_props
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_unset_props_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_unset_props_test (
          id INT,
          value STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.checkpointInterval' = '10', 'my.tag' = 'remove-me')
        """
      Given statement
        """
        INSERT INTO delta_alter_table_unset_props_test VALUES (1, 'v0')
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_unset_props_test
        UNSET TBLPROPERTIES IF EXISTS ('my.tag')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                        | value |
        | metaData.configuration['delta.checkpointInterval']          | "10"  |

  @sail-only
  Rule: ALTER TABLE SET TBLPROPERTIES can enable in-commit timestamps on an existing Delta table

    Background:
      Given variable location for temporary directory delta_alter_table_enable_ict
      Given variable delta_log for delta log of location
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_enable_ict_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_enable_ict_test
        USING DELTA
        LOCATION {{ location.sql }}
        AS SELECT 1 AS id, 'v0' AS value
        """
      Given statement
        """
        INSERT INTO delta_alter_table_enable_ict_test VALUES (2, 'v1')
        """
      Given statement
        """
        INSERT INTO delta_alter_table_enable_ict_test VALUES (3, 'v2')
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_enable_ict_test
        SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')
        """
      Given delta log commit and checksum timestamps for versions 0, 1, 2 in delta_log are 10000000, 20000000, 30000000 milliseconds since epoch
      Given delta log JSON file timestamps for versions 0, 1, 2, 3 in delta_log are 100, 200, 86400, 86400 seconds since epoch

    Scenario: Delta log protocol is upgraded and ICT enablement metadata is written on ALTER TABLE
      Then delta log latest effective protocol and metadata contains
        | path                                                                       | value  |
        | protocol.minWriterVersion                                                  | 7      |
        | protocol.writerFeatures                                                    | ["inCommitTimestamp"] |
        | metaData.configuration['delta.enableInCommitTimestamps']                   | "true" |

    Scenario: Time travel ignores pre-enablement in-commit timestamps after ALTER TABLE enables ICT
      When query
        """
        SELECT * FROM delta_alter_table_enable_ict_test
        TIMESTAMP AS OF '1970-01-01T00:02:30Z'
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |

      When query
        """
        SELECT * FROM delta_alter_table_enable_ict_test
        TIMESTAMP AS OF '1970-01-01T00:04:10Z'
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | v0    |
        | 2  | v1    |

  @sail-only
  Rule: ALTER TABLE SET TBLPROPERTIES can enable type widening on an existing Delta table

    Scenario: Delta log protocol is upgraded when ALTER TABLE enables type widening
      Given variable location for temporary directory delta_alter_table_enable_type_widening
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_enable_type_widening_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_enable_type_widening_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_alter_table_enable_type_widening_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_enable_type_widening_test
        SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                | value  |
        | protocol.minReaderVersion                           | 3      |
        | protocol.minWriterVersion                           | 7      |
        | protocol.readerFeatures                             | ["typeWidening"] |
        | protocol.writerFeatures                             | ["typeWidening"] |
        | metaData.configuration['delta.enableTypeWidening']  | "true" |

    Scenario: ALTER COLUMN TYPE applies Delta type widening and records metadata
      Given variable location for temporary directory delta_alter_column_type_widening
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_column_type_widening_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_column_type_widening_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_widening_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_widening_test
        ALTER COLUMN id TYPE BIGINT
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_widening_test VALUES (2147483648)
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                            | value     |
        | metaData.schemaString.fields[0].type                            | "long"    |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fromType | "integer" |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].toType   | "long"    |
      When query
        """
        SELECT id FROM delta_alter_column_type_widening_test ORDER BY id
        """
      Then query result ordered
        | id         |
        | 1          |
        | 2147483648 |

    Scenario: ALTER COLUMN TYPE works after type widening is enabled by ALTER TABLE
      Given variable location for temporary directory delta_alter_column_type_after_enable
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_column_type_after_enable_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_column_type_after_enable_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_after_enable_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_after_enable_test
        SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_after_enable_test
        ALTER COLUMN id TYPE BIGINT
        """
      Then delta log latest commit info contains
        | path                                                                      | value     |
        | operation                                                                 | "CHANGE COLUMN" |
        | operationParameters.column.name                                           | "id"      |
        | operationParameters.column.type                                           | "long"    |
        | operationParameters.column.metadata                                       | {}        |
      Then delta log latest effective protocol and metadata contains
        | path                                                            | value     |
        | protocol.minReaderVersion                                       | 3         |
        | protocol.minWriterVersion                                       | 7         |
        | protocol.readerFeatures                                         | ["typeWidening"] |
        | protocol.writerFeatures                                         | ["typeWidening"] |
        | metaData.schemaString.fields[0].type                            | "long"    |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fromType | "integer" |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].toType   | "long"    |

    Scenario: ALTER COLUMN TYPE commit info omits previous type widening metadata
      Given variable location for temporary directory delta_alter_column_type_commit_info_metadata
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_column_type_commit_info_metadata_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_column_type_commit_info_metadata_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_commit_info_metadata_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_commit_info_metadata_test
        ALTER COLUMN id TYPE BIGINT
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_commit_info_metadata_test
        ALTER COLUMN id TYPE DECIMAL(20, 0)
        """
      Then delta log latest commit info contains
        | path                                                                     | value           |
        | operation                                                                | "CHANGE COLUMN" |
        | operationParameters.column.name                                          | "id"            |
        | operationParameters.column.type                                          | "decimal(20,0)" |
        | operationParameters.column.metadata['delta.typeChanges'][0].fromType     | "integer"      |
        | operationParameters.column.metadata['delta.typeChanges'][0].toType       | "long"         |
      Then delta log latest effective protocol and metadata contains
        | path                                                                     | value           |
        | metaData.schemaString.fields[0].type                                     | "decimal(20,0)" |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fromType | "integer"      |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].toType   | "long"         |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][1].fromType | "long"         |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][1].toType   | "decimal(20,0)" |

    Scenario: ALTER COLUMN TYPE records metadata for a widened map key
      Given variable location for temporary directory delta_alter_column_type_map_key
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_column_type_map_key_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_column_type_map_key_test (
          attrs MAP<INT, STRING>
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_map_key_test VALUES (map(1, 'one'))
        """
      Given statement
        """
        ALTER TABLE delta_alter_column_type_map_key_test
        ALTER COLUMN attrs.key TYPE BIGINT
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                                     | value     |
        | metaData.schemaString.fields[0].type.keyType                             | "long"    |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fromType | "integer" |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].toType   | "long"    |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fieldPath | "key"     |

    Scenario: CHANGE COLUMN TYPE uses the same Delta type widening path
      Given variable location for temporary directory delta_change_column_type_widening
      Given final statement
        """
        DROP TABLE IF EXISTS delta_change_column_type_widening_test
        """
      Given statement template
        """
        CREATE TABLE delta_change_column_type_widening_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
        """
      Given statement
        """
        INSERT INTO delta_change_column_type_widening_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_change_column_type_widening_test
        CHANGE COLUMN id TYPE BIGINT
        """
      Then delta log latest effective protocol and metadata contains
        | path                                                            | value     |
        | metaData.schemaString.fields[0].type                            | "long"    |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].fromType | "integer" |
        | metaData.schemaString.fields[0].metadata['delta.typeChanges'][0].toType   | "long"    |

    Scenario: ALTER COLUMN TYPE rejects widening when the table property is disabled
      Given variable location for temporary directory delta_alter_column_type_widening_disabled
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_column_type_widening_disabled_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_column_type_widening_disabled_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_alter_column_type_widening_disabled_test VALUES (1)
        """
      Given statement with error delta.enableTypeWidening=true
        """
        ALTER TABLE delta_alter_column_type_widening_disabled_test
        ALTER COLUMN id TYPE BIGINT
        """

  @sail-only
  Rule: ALTER TABLE UNSET TBLPROPERTIES records a dedicated Delta log operation

    Scenario: Delta log commit info records UNSET TBLPROPERTIES operation on ALTER TABLE UNSET
      Given variable location for temporary directory delta_alter_table_unset_op
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_unset_op_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_unset_op_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('my.tag' = 'remove-me')
        """
      Given statement
        """
        INSERT INTO delta_alter_table_unset_op_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_unset_op_test
        UNSET TBLPROPERTIES ('my.tag')
        """
      Then delta log latest commit info contains
        | path                                     | value         |
        | operation                                | "UNSET TBLPROPERTIES" |
        | operationParameters.properties           | ["my.tag"]    |

  @sail-only
  Rule: ALTER TABLE UNSET TBLPROPERTIES validates property existence unless IF EXISTS is specified

    Scenario: ALTER TABLE UNSET TBLPROPERTIES on a missing property fails without IF EXISTS
      Given variable location for temporary directory delta_alter_table_unset_missing
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_unset_missing_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_unset_missing_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_alter_table_unset_missing_test VALUES (1)
        """
      Given statement with error not set on the table
        """
        ALTER TABLE delta_alter_table_unset_missing_test
        UNSET TBLPROPERTIES ('does.not.exist')
        """

    Scenario: ALTER TABLE UNSET TBLPROPERTIES IF EXISTS on a missing property is a no-op
      Given variable location for temporary directory delta_alter_table_unset_if_exists
      Given final statement
        """
        DROP TABLE IF EXISTS delta_alter_table_unset_if_exists_test
        """
      Given statement template
        """
        CREATE TABLE delta_alter_table_unset_if_exists_test (
          id INT
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES ('keep.me' = 'yes')
        """
      Given statement
        """
        INSERT INTO delta_alter_table_unset_if_exists_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE delta_alter_table_unset_if_exists_test
        UNSET TBLPROPERTIES IF EXISTS ('does.not.exist')
        """
      Then delta log latest effective protocol and metadata contains
        | path                                       | value |
        | metaData.configuration['keep.me']          | "yes" |
