Feature: Glue catalog table operations

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS table_test_db
      """
    Given statement
      """
      USE DATABASE table_test_db
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS table_test_db CASCADE
      """

  Scenario: Create a table with multiple column types
    Given statement
      """
      CREATE TABLE products (
        id BIGINT NOT NULL COMMENT 'Primary key',
        name STRING,
        price DOUBLE,
        category STRING NOT NULL
      )
      USING parquet
      COMMENT 'Product catalog table'
      LOCATION 's3://bucket/products'
      PARTITIONED BY (category)
      TBLPROPERTIES (owner = 'test_user')
      """
    When query
      """
      SHOW TABLES LIKE 'products'
      """
    Then query result
      | database       | tableName | isTemporary |
      | table_test_db  | products  | false       |

  Scenario: Create duplicate table fails
    Given statement
      """
      CREATE TABLE dup_table (id INT) USING parquet LOCATION 's3://bucket/dup'
      """
    Given statement with error .*
      """
      CREATE TABLE dup_table (id INT) USING parquet LOCATION 's3://bucket/dup2'
      """

  Scenario: Create table with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE TABLE ine_table (id INT)
      USING parquet
      COMMENT 'original'
      LOCATION 's3://bucket/ine'
      """
    Given statement
      """
      CREATE TABLE IF NOT EXISTS ine_table (id INT)
      USING parquet
      COMMENT 'new comment'
      LOCATION 's3://bucket/ine2'
      """
    When query
      """
      SHOW TABLES LIKE 'ine_table'
      """
    Then query result
      | database       | tableName | isTemporary |
      | table_test_db  | ine_table | false       |

  Scenario: Describe non-existent table raises error
    When query
      """
      DESCRIBE TABLE nonexistent_table_glue
      """
    Then query error .*

  Scenario: Describe existing table shows columns
    Given statement
      """
      CREATE TABLE get_test_table (
        id BIGINT NOT NULL COMMENT 'The ID',
        value STRING
      )
      USING parquet
      COMMENT 'Test table description'
      LOCATION 's3://bucket/test_table'
      TBLPROPERTIES (key1 = 'value1')
      """
    When query
      """
      SHOW TABLES LIKE 'get_test_table'
      """
    Then query result
      | database       | tableName      | isTemporary |
      | table_test_db  | get_test_table | false       |
    When query
      """
      DESCRIBE TABLE get_test_table
      """
    Then query result has row where "col_name" is "id"
    Then query result row where "col_name" is "id" has "data_type" equal to "bigint"
    Then query result row where "col_name" is "id" has "comment" equal to "The ID"
    Then query result has row where "col_name" is "value"
    Then query result row where "col_name" is "value" has "data_type" equal to "string"

  Scenario: Create table with all supported column types
    Given statement
      """
      CREATE TABLE all_types (
        col_boolean BOOLEAN,
        col_tinyint TINYINT,
        col_smallint SMALLINT,
        col_int INT,
        col_bigint BIGINT,
        col_float FLOAT,
        col_double DOUBLE,
        col_string STRING,
        col_binary BINARY,
        col_date DATE,
        col_timestamp TIMESTAMP,
        col_decimal DECIMAL(10, 2),
        col_array ARRAY<STRING>,
        col_struct STRUCT<name: STRING, value: INT>,
        col_map MAP<STRING, INT>
      )
      USING parquet
      LOCATION 's3://bucket/all_types'
      """
    When query
      """
      SHOW TABLES LIKE 'all_types'
      """
    Then query result
      | database       | tableName | isTemporary |
      | table_test_db  | all_types | false       |
    When query
      """
      DESCRIBE TABLE all_types
      """
    Then query result has row where "col_name" is "col_boolean"
    Then query result has row where "col_name" is "col_tinyint"
    Then query result has row where "col_name" is "col_smallint"
    Then query result has row where "col_name" is "col_int"
    Then query result has row where "col_name" is "col_bigint"
    Then query result has row where "col_name" is "col_float"
    Then query result has row where "col_name" is "col_double"
    Then query result has row where "col_name" is "col_string"
    Then query result has row where "col_name" is "col_binary"
    Then query result has row where "col_name" is "col_date"
    Then query result has row where "col_name" is "col_timestamp"
    Then query result has row where "col_name" is "col_decimal"
    Then query result has row where "col_name" is "col_array"
    Then query result has row where "col_name" is "col_struct"
    Then query result has row where "col_name" is "col_map"

  Scenario: Invalid DDL is rejected
    When query
      """
      CREATE TABLE unsupported_type (col_union UNIONTYPE<INT, STRING>)
      USING parquet
      LOCATION 's3://bucket/unsupported'
      """
    Then query error .*

  Scenario Outline: Create table with storage format
    Given final statement
      """
      DROP TABLE IF EXISTS test_<fmt>_table
      """
    Given statement
      """
      CREATE TABLE test_<fmt>_table (id INT NOT NULL, name STRING)
      USING <fmt>
      COMMENT 'Table with <fmt> format'
      LOCATION 's3://bucket/test_<fmt>_table'
      """
    When query
      """
      SHOW TABLES LIKE 'test_<fmt>_table'
      """
    Then query result
      | database       | tableName        | isTemporary |
      | table_test_db  | test_<fmt>_table | false       |

    Examples:
      | fmt     |
      | parquet |
      | csv     |
      | json    |
      | orc     |
      | avro    |

  Scenario: List tables in a database
    Given statement
      """
      CREATE TABLE table_alpha (id INT) USING parquet LOCATION 's3://bucket/table_alpha'
      """
    Given statement
      """
      CREATE TABLE table_beta (id INT) USING parquet LOCATION 's3://bucket/table_beta'
      """
    Given statement
      """
      CREATE TABLE table_gamma (id INT) USING parquet LOCATION 's3://bucket/table_gamma'
      """
    When query
      """
      SHOW TABLES
      """
    Then query result
      | database       | tableName   | isTemporary |
      | table_test_db  | table_alpha | false       |
      | table_test_db  | table_beta  | false       |
      | table_test_db  | table_gamma | false       |

  Scenario: Drop existing table removes it
    Given statement
      """
      CREATE TABLE drop_me (id INT) USING parquet LOCATION 's3://bucket/drop_me'
      """
    Given final statement
      """
      DROP TABLE IF EXISTS drop_me
      """
    Given statement
      """
      DROP TABLE drop_me
      """
    When query
      """
      SHOW TABLES LIKE 'drop_me'
      """
    Then query result
      | database  | tableName | isTemporary |

  Scenario: Drop non-existent table fails
    Given statement with error .*
      """
      DROP TABLE nonexistent_drop_table
      """

  Scenario: Drop non-existent table with IF EXISTS does not raise error
    Given statement
      """
      DROP TABLE IF EXISTS nonexistent_drop_table
      """

  Scenario: Hive format rejects non-identity partition transforms
    Given statement with error .*
      """
      CREATE TABLE day_part_table (id INT, ts TIMESTAMP)
      USING parquet
      PARTITIONED BY (days(ts))
      LOCATION 's3://bucket/day_part'
      """

  Scenario: Iceberg format requires location
    Given statement with error .*
      """
      CREATE TABLE iceberg_no_loc (id INT)
      USING iceberg
      """

  Scenario Outline: Round-trip primitive column types via DESCRIBE
    Given final statement
      """
      DROP TABLE IF EXISTS rt_<col>_table
      """
    Given statement
      """
      CREATE TABLE rt_<col>_table (c <spark_type>)
      USING parquet
      LOCATION 's3://bucket/rt_<col>_table'
      """
    When query
      """
      DESCRIBE TABLE rt_<col>_table
      """
    Then query result row where "col_name" is "c" has "data_type" equal to "<expected>"

    Examples:
      | col       | spark_type | expected  |
      | boolean   | BOOLEAN    | boolean   |
      | tinyint   | TINYINT    | tinyint   |
      | smallint  | SMALLINT   | smallint  |
      | int       | INT        | int       |
      | bigint    | BIGINT     | bigint    |
      | float     | FLOAT      | float     |
      | double    | DOUBLE     | double    |
      | string    | STRING     | string    |
      | binary    | BINARY     | binary    |
      | date      | DATE       | date      |
      | timestamp | TIMESTAMP  | timestamp_ntz |

  Scenario: Decimal with precision and scale round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE dec_types (
        d_default DECIMAL,
        d_small DECIMAL(10, 2),
        d_large DECIMAL(38, 18)
      )
      USING parquet
      LOCATION 's3://bucket/dec_types'
      """
    When query
      """
      DESCRIBE TABLE dec_types
      """
    Then query result row where "col_name" is "d_small" has "data_type" equal to "decimal(10,2)"
    Then query result row where "col_name" is "d_large" has "data_type" equal to "decimal(38,18)"
    Then query result row where "col_name" is "d_default" has "data_type" containing "decimal"

  Scenario: Array of primitive round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE arr_prim (ids ARRAY<INT>)
      USING parquet
      LOCATION 's3://bucket/arr_prim'
      """
    When query
      """
      DESCRIBE TABLE arr_prim
      """
    Then query result row where "col_name" is "ids" has "data_type" containing "array<int>"

  Scenario: Nested array of array round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE arr_nested (matrix ARRAY<ARRAY<STRING>>)
      USING parquet
      LOCATION 's3://bucket/arr_nested'
      """
    When query
      """
      DESCRIBE TABLE arr_nested
      """
    Then query result row where "col_name" is "matrix" has "data_type" containing "array<array<string>>"

  Scenario: Struct with multiple fields round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE struct_cols (person STRUCT<name: STRING, age: INT>)
      USING parquet
      LOCATION 's3://bucket/struct_cols'
      """
    When query
      """
      DESCRIBE TABLE struct_cols
      """
    Then query result row where "col_name" is "person" has "data_type" containing "struct<"
    Then query result row where "col_name" is "person" has "data_type" containing "name:string"
    Then query result row where "col_name" is "person" has "data_type" containing "age:int"

  Scenario: Map with primitive key and value round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE map_prim (props MAP<STRING, INT>)
      USING parquet
      LOCATION 's3://bucket/map_prim'
      """
    When query
      """
      DESCRIBE TABLE map_prim
      """
    Then query result row where "col_name" is "props" has "data_type" containing "map<string,int>"

  Scenario: Map with array value round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE map_of_array (tags MAP<STRING, ARRAY<INT>>)
      USING parquet
      LOCATION 's3://bucket/map_of_array'
      """
    When query
      """
      DESCRIBE TABLE map_of_array
      """
    Then query result row where "col_name" is "tags" has "data_type" containing "map<string,array<int>>"

  Scenario: Deeply nested array of struct with inner list round-trips via DESCRIBE
    Given statement
      """
      CREATE TABLE deep_nested (
        items ARRAY<STRUCT<id: BIGINT, tags: ARRAY<STRING>>>
      )
      USING parquet
      LOCATION 's3://bucket/deep_nested'
      """
    When query
      """
      DESCRIBE TABLE deep_nested
      """
    Then query result row where "col_name" is "items" has "data_type" containing "array<struct<"
    Then query result row where "col_name" is "items" has "data_type" containing "id:bigint"
    Then query result row where "col_name" is "items" has "data_type" containing "tags:array<string>"

  Scenario: NOT NULL column constraint is preserved through Glue round-trip
    Given statement
      """
      CREATE TABLE nn_table (
        id BIGINT NOT NULL COMMENT 'required',
        name STRING
      )
      USING parquet
      LOCATION 's3://bucket/nn_table'
      """
    When query
      """
      DESCRIBE TABLE nn_table
      """
    Then query result row where "col_name" is "id" has "data_type" equal to "bigint"
    Then query result row where "col_name" is "id" has "comment" equal to "required"
    Then query result row where "col_name" is "name" has "data_type" equal to "string"

  Scenario: Lowercase Spark type keywords are accepted
    Given statement
      """
      CREATE TABLE lower_case_types (a int, b bigint, c string)
      USING parquet
      LOCATION 's3://bucket/lower_case_types'
      """
    When query
      """
      DESCRIBE TABLE lower_case_types
      """
    Then query result row where "col_name" is "a" has "data_type" equal to "int"
    Then query result row where "col_name" is "b" has "data_type" equal to "bigint"
    Then query result row where "col_name" is "c" has "data_type" equal to "string"

  Scenario: SELECT from empty Glue-registered table returns no rows
    Given statement
      """
      CREATE TABLE empty_select (id BIGINT, name STRING)
      USING parquet
      LOCATION 'file:///tmp/sail_glue_empty_select'
      """
    When query
      """
      SELECT id, name FROM empty_select
      """
    Then query result
      | id | name |
