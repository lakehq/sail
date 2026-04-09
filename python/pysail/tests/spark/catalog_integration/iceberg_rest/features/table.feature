Feature: Iceberg REST catalog table operations

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS iceberg_table_test
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS iceberg_table_test CASCADE
      """

  Scenario: Create a table with columns and comment
    Given statement
      """
      CREATE TABLE iceberg_table_test.t1 (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'peow'
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 't1'
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | t1        | false       |

  Scenario: Create duplicate table fails
    Given statement
      """
      CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg
      """
    Given statement with error .*
      """
      CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg
      """

  Scenario: Create table with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE TABLE iceberg_table_test.ine_t (id INT) USING iceberg
      """
    Given statement
      """
      CREATE TABLE IF NOT EXISTS iceberg_table_test.ine_t (id INT) USING iceberg
      """

  Scenario: Create a partitioned table
    Given statement
      """
      CREATE TABLE iceberg_table_test.partitioned_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'test table'
      PARTITIONED BY (baz)
      LOCATION 's3://icebergdata/custom/path/meow'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'partitioned_t'
      """
    Then query result
      | database           | tableName    | isTemporary |
      | iceberg_table_test | partitioned_t | false       |

  Scenario: Describe existing table shows columns
    Given statement
      """
      CREATE TABLE iceberg_table_test.get_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'test table'
      PARTITIONED BY (baz)
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'get_t'
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | get_t     | false       |

  Scenario: Describe non-existent table raises error
    When query
      """
      DESCRIBE TABLE iceberg_table_test.nonexistent_t
      """
    Then query error .*

  Scenario: List tables in a namespace
    Given statement
      """
      CREATE TABLE iceberg_table_test.list_t1 (id INT) USING iceberg
      """
    Given statement
      """
      CREATE TABLE iceberg_table_test.list_t2 (id INT) USING iceberg
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | list_t1   | false       |
      | iceberg_table_test | list_t2   | false       |

  Scenario: Drop existing table removes it
    Given statement
      """
      CREATE TABLE iceberg_table_test.drop_t (id INT) USING iceberg
      """
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.drop_t
      """
    Given statement
      """
      DROP TABLE iceberg_table_test.drop_t
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.drop_t
      """
    Then query error .*

  Scenario: Drop non-existent table with IF EXISTS does not raise error
    Given statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.nonexistent_drop_t
      """

  Scenario: Create a table with year partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.year_part_t (
        id INT,
        event_date DATE
      )
      USING iceberg
      PARTITIONED BY (years(event_date))
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'year_part_t'
      """
    Then query result
      | database           | tableName   | isTemporary |
      | iceberg_table_test | year_part_t | false       |

  Scenario: Create a table with bucket partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.bucket_part_t (
        user_id INT,
        name STRING
      )
      USING iceberg
      PARTITIONED BY (bucket(4, user_id))
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'bucket_part_t'
      """
    Then query result
      | database           | tableName     | isTemporary |
      | iceberg_table_test | bucket_part_t | false       |
