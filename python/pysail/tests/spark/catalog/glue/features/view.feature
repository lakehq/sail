Feature: Glue catalog view operations

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS view_test_db
      """
    Given statement
      """
      USE DATABASE view_test_db
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS view_test_db CASCADE
      """

  Scenario: Create a view
    Given statement
      """
      CREATE VIEW product_view
      COMMENT 'View of products'
      TBLPROPERTIES (owner = 'test_user')
      AS SELECT 1 AS id, 'test' AS name, 9.99 AS price
      """
    When query
      """
      SELECT * FROM product_view LIMIT 1
      """
    Then query result
      | id | name | price |
      | 1  | test | 9.99  |

  Scenario: Create duplicate view fails
    Given statement
      """
      CREATE VIEW dup_view AS SELECT 1 AS id
      """
    Given statement with error .*
      """
      CREATE VIEW dup_view AS SELECT 2 AS id
      """

  Scenario: Create view with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE VIEW ine_view COMMENT 'original' AS SELECT 1 AS id
      """
    Given statement
      """
      CREATE VIEW IF NOT EXISTS ine_view COMMENT 'new' AS SELECT 2 AS id
      """

  Scenario: Describe non-existent view raises error
    When query
      """
      DESCRIBE TABLE nonexistent_view_glue
      """
    Then query error .*

  Scenario: Query from an existing view returns its rows
    Given statement
      """
      CREATE VIEW test_view
      COMMENT 'Test view description'
      TBLPROPERTIES (key1 = 'value1')
      AS SELECT 1 AS id, 'hello' AS value
      """
    When query
      """
      SELECT * FROM test_view LIMIT 1
      """
    Then query result
      | id | value |
      | 1  | hello |

  Scenario: Views coexist with tables in the same database
    Given statement
      """
      CREATE VIEW view_alpha AS SELECT 1 AS id
      """
    Given statement
      """
      CREATE VIEW view_beta AS SELECT 2 AS id
      """
    Given statement
      """
      CREATE TABLE a_table (id INT) USING parquet LOCATION 's3://bucket/a_table'
      """
    When query
      """
      SELECT * FROM view_alpha LIMIT 1
      """
    Then query result
      | id |
      | 1  |
    When query
      """
      SHOW TABLES LIKE 'a_table'
      """
    Then query result
      | database     | tableName | isTemporary |
      | view_test_db | a_table   | false       |

  Scenario: Drop existing view removes it
    Given statement
      """
      CREATE VIEW drop_me_view AS SELECT 1 AS id
      """
    Given final statement
      """
      DROP VIEW IF EXISTS drop_me_view
      """
    Given statement
      """
      DROP VIEW drop_me_view
      """
    When query
      """
      DESCRIBE TABLE drop_me_view
      """
    Then query error .*

  Scenario: Drop non-existent view fails
    Given statement with error .*
      """
      DROP VIEW nonexistent_drop_view
      """

  Scenario: Drop non-existent view with IF EXISTS does not raise error
    Given statement
      """
      DROP VIEW IF EXISTS nonexistent_drop_view
      """

  Scenario: Describe table returns table schema not view schema
    Given statement
      """
      CREATE TABLE get_view_table (id INT) USING parquet LOCATION 's3://bucket/get_view_table'
      """
    Given statement
      """
      CREATE VIEW get_view_target AS SELECT CAST(1 AS BIGINT) AS id
      """
    When query
      """
      DESCRIBE TABLE get_view_table
      """
    Then query result
      | col_name | data_type | comment |
      | id       | int       | NULL    |
