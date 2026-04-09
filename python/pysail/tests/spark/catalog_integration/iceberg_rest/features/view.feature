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
      SELECT * FROM iceberg_view_test.list_v1
      """
    Then query result
      | id |
      | 1  |
    When query
      """
      SELECT * FROM iceberg_view_test.list_v2
      """
    Then query result
      | id |
      | 2  |
