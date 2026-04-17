Feature: Persistent views

  Background:
    Given final statement
      """
      DROP TABLE IF EXISTS view_customers
      """
    Given final statement
      """
      DROP VIEW IF EXISTS customer_view
      """
    Given statement
      """
      CREATE TABLE view_customers (id INT, name STRING)
      """
    Given statement
      """
      INSERT INTO view_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')
      """
    Given statement
      """
      CREATE VIEW customer_view AS SELECT * FROM view_customers
      """

  Scenario: Read from a persistent view
    When query
      """
      SELECT * FROM customer_view ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |
      | 3  | Carol |

  Scenario: SHOW TABLES lists persistent views
    When query
      """
      SHOW TABLES LIKE 'customer_view'
      """
    Then query result
      | database | tableName     | isTemporary |
      | default  | customer_view | false       |

  Scenario: DESCRIBE lists columns of persistent views
    When query
      """
      DESCRIBE customer_view
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | int       | NULL    |
      | name     | string    | NULL    |
