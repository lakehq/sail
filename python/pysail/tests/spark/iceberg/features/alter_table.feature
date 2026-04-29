Feature: Iceberg ALTER TABLE SET/UNSET TBLPROPERTIES

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
