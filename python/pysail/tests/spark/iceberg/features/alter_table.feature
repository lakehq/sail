Feature: Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES

  @sail-only
  Rule: CREATE TABLE TBLPROPERTIES are materialized into Iceberg metadata

    Scenario: CREATE TABLE TBLPROPERTIES writes metadata properties on first commit
      Given variable location for temporary directory iceberg_create_table_props
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_create_table_props_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_create_table_props_test (
          id INT,
          value STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('write.data.path' = 'custom_data', 'custom.created' = 'yes')
        """
      Given statement
        """
        INSERT INTO iceberg_create_table_props_test VALUES (1, 'v0')
        """
      Then iceberg metadata contains
        | path                                  | value         |
        | properties['write.data.path']         | "custom_data" |
        | properties['custom.created']          | "yes"        |
      Then file tree in location matches
        """
        📂 custom_data
          📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 snap-*.avro
        """

    Scenario: CREATE TABLE format-version selects metadata version without persisting reserved properties
      Given variable location for temporary directory iceberg_create_table_format_version
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_create_table_format_version_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_create_table_format_version_test (
          id INT
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('format-version' = '1', 'uuid' = 'not-persisted', 'custom.created' = 'yes')
        """
      Given statement
        """
        INSERT INTO iceberg_create_table_format_version_test VALUES (1)
        """
      Then iceberg metadata contains
        | path               | value                    |
        | $['format-version'] | 1                        |
        | properties         | {"custom.created":"yes"} |

  @sail-only
  Rule: ALTER TABLE SET TBLPROPERTIES persists properties to Iceberg metadata

    Scenario: ALTER TABLE SET TBLPROPERTIES writes a new metadata file
      Given variable location for temporary directory iceberg_alter_table_set_props
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_alter_table_set_props_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_alter_table_set_props_test (
          id INT,
          value STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_alter_table_set_props_test VALUES (1, 'v0')
        """
      Given statement
        """
        ALTER TABLE iceberg_alter_table_set_props_test
        SET TBLPROPERTIES ('write.metadata.previous-versions-max' = '7', 'custom.key' = 'hello')
        """
      Then iceberg metadata contains
        | path                                                   | value   |
        | properties['write.metadata.previous-versions-max']     | "7"     |
        | properties['custom.key']                               | "hello" |

    Scenario: ALTER TABLE SET TBLPROPERTIES upgrades format version without persisting reserved properties
      Given variable location for temporary directory iceberg_alter_table_format_version
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_alter_table_format_version_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_alter_table_format_version_test (
          id INT
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('format-version' = '1', 'custom.created' = 'yes')
        """
      Given statement
        """
        INSERT INTO iceberg_alter_table_format_version_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE iceberg_alter_table_format_version_test
        SET TBLPROPERTIES ('format-version' = '2', 'uuid' = 'not-persisted', 'custom.key' = 'hello')
        """
      Then iceberg metadata contains
        | path               | value                                        |
        | $['format-version'] | 2                                            |
        | properties         | {"custom.created":"yes","custom.key":"hello"} |

  @sail-only
  Rule: ALTER TABLE UNSET TBLPROPERTIES persists removals to Iceberg metadata

    Scenario: ALTER TABLE UNSET TBLPROPERTIES removes a property
      Given variable location for temporary directory iceberg_alter_table_unset_props
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_alter_table_unset_props_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_alter_table_unset_props_test (
          id INT,
          value STRING
        )
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_alter_table_unset_props_test VALUES (1, 'v0')
        """
      Given statement
        """
        ALTER TABLE iceberg_alter_table_unset_props_test
        SET TBLPROPERTIES ('keep.me' = 'yes', 'remove.me' = 'no')
        """
      Given statement
        """
        ALTER TABLE iceberg_alter_table_unset_props_test
        UNSET TBLPROPERTIES IF EXISTS ('remove.me')
        """
      Then iceberg metadata contains
        | path       | value              |
        | properties | {"keep.me":"yes"} |

    Scenario: ALTER TABLE UNSET TBLPROPERTIES on a missing property fails without IF EXISTS
      Given variable location for temporary directory iceberg_alter_table_unset_missing
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_alter_table_unset_missing_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_alter_table_unset_missing_test (
          id INT
        )
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO iceberg_alter_table_unset_missing_test VALUES (1)
        """
      Given statement with error not set on the table
        """
        ALTER TABLE iceberg_alter_table_unset_missing_test
        UNSET TBLPROPERTIES ('does.not.exist')
        """

    Scenario: ALTER TABLE UNSET TBLPROPERTIES IF EXISTS on a missing property is a no-op
      Given variable location for temporary directory iceberg_alter_table_unset_if_exists
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_alter_table_unset_if_exists_test
        """
      Given statement template
        """
        CREATE TABLE iceberg_alter_table_unset_if_exists_test (
          id INT
        )
        USING iceberg
        LOCATION {{ location.uri }}
        TBLPROPERTIES ('keep.me' = 'yes')
        """
      Given statement
        """
        INSERT INTO iceberg_alter_table_unset_if_exists_test VALUES (1)
        """
      Given statement
        """
        ALTER TABLE iceberg_alter_table_unset_if_exists_test
        UNSET TBLPROPERTIES IF EXISTS ('does.not.exist')
        """
      Then iceberg metadata contains
        | path       | value             |
        | properties | {"keep.me":"yes"} |
