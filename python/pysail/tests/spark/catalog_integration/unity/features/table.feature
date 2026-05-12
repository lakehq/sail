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

  Scenario: Drop existing table removes it
    Given statement
      """
      CREATE TABLE unity_table_test.drop_me (id INT)
      USING delta
      LOCATION 's3://deltadata/drop_me'
      """
    Given final statement
      """
      DROP TABLE IF EXISTS unity_table_test.drop_me
      """
    Given statement
      """
      DROP TABLE unity_table_test.drop_me
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.drop_me
      """
    Then query error .*

  Scenario: Drop non-existent table fails
    Given statement with error .*
      """
      DROP TABLE unity_table_test.nonexistent_drop_t
      """

  Scenario: Drop non-existent table with IF EXISTS does not raise error
    Given statement
      """
      DROP TABLE IF EXISTS unity_table_test.nonexistent_drop_t
      """

  Scenario: Describe table shows column metadata and comment
    Given statement
      """
      CREATE TABLE unity_table_test.describe_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING delta
      COMMENT 'test table'
      PARTITIONED BY (baz)
      LOCATION 's3://deltadata/custom/path/describe_test'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.describe_t
      """
    Then query result has row where "col_name" is "foo"
    Then query result has row where "col_name" is "bar"
    Then query result has row where "col_name" is "baz"
    Then query result row where "col_name" is "foo" has "data_type" equal to "string"
    Then query result row where "col_name" is "bar" has "data_type" equal to "int"
    Then query result row where "col_name" is "baz" has "data_type" equal to "boolean"
    Then query result row where "col_name" is "bar" has "comment" equal to "meow"

  Scenario: Describe table shows complex column types
    Given statement
      """
      CREATE TABLE unity_table_test.describe_complex_t (
        foo STRING,
        bar ARRAY<STRUCT<a: STRING, b: INT>> NOT NULL COMMENT 'meow',
        baz MAP<STRING, INT>,
        mew STRUCT<a: STRING, b: INT>
      )
      USING delta
      COMMENT 'peow'
      LOCATION 's3://deltadata/custom/path/describe_complex'
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.describe_complex_t
      """
    Then query result has row where "col_name" is "foo"
    Then query result has row where "col_name" is "bar"
    Then query result has row where "col_name" is "baz"
    Then query result has row where "col_name" is "mew"
    Then query result row where "col_name" is "bar" has "data_type" containing "array"
    Then query result row where "col_name" is "bar" has "data_type" containing "struct"
    Then query result row where "col_name" is "baz" has "data_type" containing "map"
    Then query result row where "col_name" is "mew" has "data_type" containing "struct"
    Then query result row where "col_name" is "bar" has "comment" equal to "meow"

  Scenario: Create table with storage OPTIONS
    Given statement
      """
      CREATE TABLE unity_table_test.opts_t (
        foo STRING,
        bar INT,
        baz BOOLEAN
      )
      USING delta
      COMMENT 'with options'
      PARTITIONED BY (baz)
      OPTIONS (key1 = 'value1')
      LOCATION 's3://deltadata/custom/path/opts'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'opts_t'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | opts_t    | false       |

  Scenario: Create table with primitive numeric and boolean types
    Given statement
      """
      CREATE TABLE unity_table_test.prim_num_t (
        c_int INT,
        c_bigint BIGINT,
        c_float FLOAT,
        c_double DOUBLE,
        c_bool BOOLEAN
      )
      USING delta
      LOCATION 's3://deltadata/custom/path/prim_num'
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.prim_num_t
      """
    Then query result row where "col_name" is "c_int" has "data_type" equal to "int"
    Then query result row where "col_name" is "c_bigint" has "data_type" equal to "bigint"
    Then query result row where "col_name" is "c_float" has "data_type" equal to "float"
    Then query result row where "col_name" is "c_double" has "data_type" equal to "double"
    Then query result row where "col_name" is "c_bool" has "data_type" equal to "boolean"

  Scenario: Create table with string, binary, and decimal types
    Given statement
      """
      CREATE TABLE unity_table_test.prim_str_t (
        c_str STRING,
        c_bin BINARY,
        c_dec DECIMAL(10, 2)
      )
      USING delta
      LOCATION 's3://deltadata/custom/path/prim_str'
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.prim_str_t
      """
    Then query result row where "col_name" is "c_str" has "data_type" equal to "string"
    Then query result row where "col_name" is "c_bin" has "data_type" equal to "binary"
    Then query result row where "col_name" is "c_dec" has "data_type" containing "decimal"

  Scenario: Create table with date and timestamp types
    Given statement
      """
      CREATE TABLE unity_table_test.prim_time_t (
        c_date DATE,
        c_ts TIMESTAMP
      )
      USING delta
      LOCATION 's3://deltadata/custom/path/prim_time'
      """
    When query
      """
      DESCRIBE TABLE unity_table_test.prim_time_t
      """
    Then query result row where "col_name" is "c_date" has "data_type" equal to "date"
    Then query result row where "col_name" is "c_ts" has "data_type" containing "timestamp"

  Scenario: Dropping one table does not affect sibling tables
    Given statement
      """
      CREATE TABLE unity_table_test.sib_a (id INT)
      USING delta
      LOCATION 's3://deltadata/sib_a'
      """
    Given statement
      """
      CREATE TABLE unity_table_test.sib_b (id INT)
      USING delta
      LOCATION 's3://deltadata/sib_b'
      """
    Given statement
      """
      DROP TABLE unity_table_test.sib_a
      """
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'sib_a'
      """
    Then query result
      | database | tableName | isTemporary |
    When query
      """
      SHOW TABLES IN unity_table_test LIKE 'sib_b'
      """
    Then query result
      | database                           | tableName | isTemporary |
      | sail_test_catalog.unity_table_test | sib_b     | false       |
