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

  Scenario: Invalid DDL is rejected
    When query
      """
      CREATE TABLE unsupported_type (col_union UNIONTYPE<INT, STRING>)
      USING parquet
      LOCATION 's3://bucket/unsupported'
      """
    Then query error .*

  Scenario Outline: Create table with <fmt> storage format
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
