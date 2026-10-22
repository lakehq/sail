Feature: Iceberg REST catalog view operations

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS iceberg_view_test
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS iceberg_view_test CASCADE
      """

  Scenario: Create a view
    Given statement
      """
      CREATE VIEW iceberg_view_test.product_view
      AS SELECT 1 AS id, 'test' AS name, 9.99 AS price
      """
    When query
      """
      SELECT * FROM iceberg_view_test.product_view LIMIT 1
      """
    Then query result
      | id | name | price |
      | 1  | test | 9.99  |

  Scenario: Create duplicate view fails
    Given statement
      """
      CREATE VIEW iceberg_view_test.dup_view AS SELECT 1 AS id
      """
    Given statement with error .*
      """
      CREATE VIEW iceberg_view_test.dup_view AS SELECT 2 AS id
      """

  Scenario: Create view with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE VIEW iceberg_view_test.ine_view AS SELECT 1 AS id
      """
    Given statement
      """
      CREATE VIEW IF NOT EXISTS iceberg_view_test.ine_view AS SELECT 2 AS id
      """

  Scenario: Describe existing view shows columns
    Given statement
      """
      CREATE VIEW iceberg_view_test.desc_view
      AS SELECT 1 AS id, 'hello' AS value
      """
    When query
      """
      SELECT * FROM iceberg_view_test.desc_view LIMIT 1
      """
    Then query result
      | id | value |
      | 1  | hello |
    When query
      """
      DESCRIBE TABLE iceberg_view_test.desc_view
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "value"
    Then query result row where "col_name" is "id" has "data_type" equal to "int"
    Then query result row where "col_name" is "value" has "data_type" equal to "string"

  Scenario: Create view with explicit column list and comments
    Given statement
      """
      CREATE VIEW iceberg_view_test.cols_view (
        col1,
        col2 COMMENT 'important column'
      )
      AS SELECT 'foo' AS col1, 42 AS col2
      """
    When query
      """
      DESCRIBE TABLE iceberg_view_test.cols_view
      """
    Then query result has row where "col_name" is "col1"
    Then query result has row where "col_name" is "col2"
    Then query result row where "col_name" is "col2" has "comment" equal to "important column"

  Scenario: Describe non-existent view raises error
    When query
      """
      DESCRIBE TABLE iceberg_view_test.nonexistent_view
      """
    Then query error .*

  Scenario: Drop existing view removes it
    Given statement
      """
      CREATE VIEW iceberg_view_test.drop_me_view AS SELECT 1 AS id
      """
    Given final statement
      """
      DROP VIEW IF EXISTS iceberg_view_test.drop_me_view
      """
    Given statement
      """
      DROP VIEW iceberg_view_test.drop_me_view
      """
    When query
      """
      DESCRIBE TABLE iceberg_view_test.drop_me_view
      """
    Then query error .*

  Scenario: Drop non-existent view fails
    Given statement with error .*
      """
      DROP VIEW iceberg_view_test.nonexistent_drop_view
      """

  Scenario: Drop non-existent view with IF EXISTS does not raise error
    Given statement
      """
      DROP VIEW IF EXISTS iceberg_view_test.nonexistent_drop_view
      """

  Scenario: List views in a namespace
    Given statement
      """
      CREATE VIEW iceberg_view_test.list_v1 AS SELECT 1 AS id
      """
    Given statement
      """
      CREATE VIEW iceberg_view_test.list_v2 AS SELECT 2 AS id
      """
    When query
      """
      SHOW VIEWS IN iceberg_view_test LIKE 'list_v1'
      """
    Then query result has row where "tableName" is "list_v1"
    When query
      """
      SHOW VIEWS IN iceberg_view_test LIKE 'list_v2'
      """
    Then query result has row where "tableName" is "list_v2"

  Scenario: SHOW VIEWS without filter lists created views
    Given statement
      """
      CREATE VIEW iceberg_view_test.all_v1 AS SELECT 1 AS id
      """
    Given statement
      """
      CREATE VIEW iceberg_view_test.all_v2 AS SELECT 2 AS id
      """
    When query
      """
      SHOW VIEWS IN iceberg_view_test
      """
    Then query result has row where "tableName" is "all_v1"
    Then query result has row where "tableName" is "all_v2"

  Scenario: DESCRIBE TABLE EXTENDED on a view reports view text and comment
    Given statement
      """
      CREATE VIEW iceberg_view_test.ext_view
      COMMENT 'view comment'
      AS SELECT 1 AS id, 'hello' AS value
      """
    When query
      """
      DESCRIBE TABLE EXTENDED iceberg_view_test.ext_view
      """
    Then query result row where "col_name" is "Comment" has "data_type" equal to "view comment"
    Then query result row where "col_name" is "View Text" has "data_type" containing "SELECT"
