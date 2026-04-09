Feature: Unity Catalog table operations

  Background:
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS unity_table_test
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS unity_table_test CASCADE
      """

  Scenario: Create a table with complex column types
    Given statement
      """
      CREATE TABLE unity_table_test.t1 (
        foo STRING,
        bar ARRAY<STRUCT<a: STRING, b: INT>> NOT NULL COMMENT 'meow',
        baz MAP<STRING, INT>,
        mew STRUCT<a: STRING, b: INT>
      )
      USING delta
      COMMENT 'peow'
      LOCATION 's3://deltadata/custom/path/meow'
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 't1'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | t1        | false       |

  Scenario: Create duplicate table fails
    Given statement
      """
      CREATE TABLE unity_table_test.dup_t (id INT)
      USING delta
      LOCATION 's3://deltadata/dup_test'
      """
    Given statement with error .*
      """
      CREATE TABLE unity_table_test.dup_t (id INT)
      USING delta
      LOCATION 's3://deltadata/dup_test2'
      """

  Scenario: Create table with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE TABLE unity_table_test.ine_t (id INT)
      USING delta
      COMMENT 'original'
      LOCATION 's3://deltadata/ine_test'
      """
    Given statement
      """
      CREATE TABLE IF NOT EXISTS unity_table_test.ine_t (id INT)
      USING delta
      COMMENT 'new comment'
      LOCATION 's3://deltadata/ine_test2'
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'ine_t'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | ine_t     | false       |

  Scenario: Create a partitioned table
    Given statement
      """
      CREATE TABLE unity_table_test.t2 (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING delta
      COMMENT 'test table'
      PARTITIONED BY (baz)
      LOCATION 's3://deltadata/custom/path/meow2'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 't2'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | t2        | false       |

  Scenario: Describe existing table shows columns
    Given statement
      """
      CREATE TABLE unity_table_test.get_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING delta
      COMMENT 'test table'
      PARTITIONED BY (baz)
      LOCATION 's3://deltadata/custom/path/get_test'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'get_t'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | get_t     | false       |

  Scenario: Describe non-existent table raises error
    When query
      """
      DESCRIBE TABLE unity_table_test.nonexistent_t
      """
    Then query error .*

  Scenario: List tables in a schema
    Given statement
      """
      CREATE TABLE unity_table_test.list_t1 (id INT)
      USING delta
      LOCATION 's3://deltadata/list_t1'
      """
    Given statement
      """
      CREATE TABLE unity_table_test.list_t2 (id INT)
      USING delta
      LOCATION 's3://deltadata/list_t2'
      """
    When query
      """
      SHOW TABLES IN unity_table_test
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | list_t1   | false       |
      | sail_test_catalog.unity_table_test | list_t2   | false       |

  Scenario: List tables in empty schema returns no rows
    Given statement
      """
      CREATE SCHEMA IF NOT EXISTS empty_table_schema_unity
      """
    Given final statement
      """
      DROP SCHEMA IF EXISTS empty_table_schema_unity
      """
    When query
      """
      SHOW TABLES IN empty_table_schema_unity
      """
    Then query result
      | database | tableName | isTemporary |
