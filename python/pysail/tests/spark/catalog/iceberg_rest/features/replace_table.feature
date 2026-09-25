Feature: Iceberg REST catalog CREATE OR REPLACE / REPLACE TABLE

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS iceberg_replace_test
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS iceberg_replace_test CASCADE
      """

  Scenario: CREATE OR REPLACE changes the table schema
    Given statement
      """
      CREATE TABLE iceberg_replace_test.t_schema (
        id INT,
        name STRING
      )
      USING iceberg
      COMMENT 'original'
      """
    Given statement
      """
      CREATE OR REPLACE TABLE iceberg_replace_test.t_schema (
        id BIGINT,
        score DOUBLE,
        label STRING
      )
      USING iceberg
      COMMENT 'replaced'
      """
    When query
      """
      DESCRIBE TABLE iceberg_replace_test.t_schema
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "score"
    Then query result has row where "col_name" is "label"
    Then query result row where "col_name" is "id" has "data_type" equal to "bigint"
    Then query result row where "col_name" is "score" has "data_type" equal to "double"

  Scenario: CREATE OR REPLACE drops the old columns
    Given statement
      """
      CREATE TABLE iceberg_replace_test.t_drop_cols (
        id INT,
        name STRING
      )
      USING iceberg
      """
    Given statement
      """
      CREATE OR REPLACE TABLE iceberg_replace_test.t_drop_cols (
        id BIGINT,
        score DOUBLE
      )
      USING iceberg
      """
    When query
      """
      SELECT name FROM iceberg_replace_test.t_drop_cols
      """
    Then query error .*

  Scenario: CREATE OR REPLACE changes the partitioning
    Given statement
      """
      CREATE TABLE iceberg_replace_test.t_part (
        name STRING,
        region STRING
      )
      USING iceberg
      PARTITIONED BY (region)
      """
    Given statement
      """
      CREATE OR REPLACE TABLE iceberg_replace_test.t_part (
        event_date DATE,
        category STRING
      )
      USING iceberg
      PARTITIONED BY (category)
      """
    When query
      """
      DESCRIBE TABLE iceberg_replace_test.t_part
      """
    Then query result has row where "col_name" is "event_date"
    Then query result has row where "col_name" is "category"
    When query
      """
      SELECT region FROM iceberg_replace_test.t_part
      """
    Then query error .*

  Scenario: Replaced table reads empty after a prior INSERT
    Given statement
      """
      CREATE TABLE iceberg_replace_test.t_empty (
        id INT,
        name STRING
      )
      USING iceberg
      """
    Given statement
      """
      INSERT INTO iceberg_replace_test.t_empty VALUES (1, 'a'), (2, 'b')
      """
    Given statement
      """
      CREATE OR REPLACE TABLE iceberg_replace_test.t_empty (
        id INT,
        name STRING
      )
      USING iceberg
      """
    When query
      """
      SELECT COUNT(*) AS c FROM iceberg_replace_test.t_empty
      """
    Then query result
      | c |
      | 0 |

  Scenario: CREATE OR REPLACE on a missing table creates it
    Given statement
      """
      CREATE OR REPLACE TABLE iceberg_replace_test.t_new (
        id INT,
        name STRING
      )
      USING iceberg
      COMMENT 'fresh'
      """
    When query
      """
      SHOW TABLES IN iceberg_replace_test LIKE 't_new'
      """
    Then query result
      | database             | tableName | isTemporary |
      | iceberg_replace_test | t_new     | false       |
    When query
      """
      SELECT COUNT(*) AS c FROM iceberg_replace_test.t_new
      """
    Then query result
      | c |
      | 0 |

  Scenario: REPLACE on a missing table fails
    Given statement with error .*
      """
      REPLACE TABLE iceberg_replace_test.t_absent (
        id INT
      )
      USING iceberg
      """
