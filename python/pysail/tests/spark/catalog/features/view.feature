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

  Scenario: Create and read filtered persistent view
    Given final statement
      """
      DROP VIEW IF EXISTS active_customers
      """
    Given statement
      """
      CREATE VIEW active_customers AS SELECT * FROM view_customers WHERE id = 1
      """
    When query
      """
      SELECT * FROM active_customers ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |

  Scenario: Join with qualified columns from persistent view
    Given final statement
      """
      DROP VIEW IF EXISTS active_customers
      """
    Given final statement
      """
      DROP TABLE IF EXISTS view_orders
      """
    Given statement
      """
      CREATE VIEW active_customers AS SELECT * FROM view_customers WHERE id = 1
      """
    Given statement
      """
      CREATE TABLE view_orders (order_id INT, customer_id INT)
      """
    Given statement
      """
      INSERT INTO view_orders VALUES (100, 1), (101, 2)
      """
    When query
      """
      SELECT active_customers.name, view_orders.order_id
      FROM active_customers
      JOIN view_orders ON active_customers.id = view_orders.customer_id
      ORDER BY view_orders.order_id
      """
    Then query result ordered
      | name  | order_id |
      | Alice | 100      |

  Scenario: Create persistent view with column aliases
    Given final statement
      """
      DROP VIEW IF EXISTS aliased_customers
      """
    Given statement
      """
      CREATE VIEW aliased_customers (customer_id, customer_name) AS SELECT id, name FROM view_customers
      """
    When query
      """
      SELECT customer_id, customer_name FROM aliased_customers ORDER BY customer_id
      """
    Then query result ordered
      | customer_id | customer_name |
      | 1           | Alice         |
      | 2           | Bob           |
      | 3           | Carol         |
    When query
      """
      DESCRIBE TABLE aliased_customers
      """
    Then query result ordered
      | col_name      | data_type | comment |
      | customer_id   | int       | NULL    |
      | customer_name | string    | NULL    |

  Scenario: SHOW TABLES lists persistent views
    When query
      """
      SHOW TABLES LIKE 'customer_view'
      """
    Then query result
      | database | tableName     | isTemporary |
      | default  | customer_view | false       |

  Scenario: SHOW TABLE EXTENDED includes persistent views
    When query
      """
      SHOW TABLE EXTENDED LIKE '*'
      """
    Then query result has row where "tableName" is "view_customers"
    And query result has row where "tableName" is "customer_view"

  Scenario: SHOW TABLE EXTENDED reports persistent view type
    When query
      """
      SHOW TABLE EXTENDED LIKE 'customer_view'
      """
    Then query result row where "tableName" is "customer_view" has "information" containing "Type: VIEW"

  Scenario: SHOW TABLE EXTENDED reports persistent view schema
    When query
      """
      SHOW TABLE EXTENDED LIKE 'customer_view'
      """
    Then query result row where "tableName" is "customer_view" has "information" containing "Schema: root"
    And query result row where "tableName" is "customer_view" has "information" containing " |-- id: int (nullable = "
    And query result row where "tableName" is "customer_view" has "information" containing " |-- name: string (nullable = "

  Scenario: DESCRIBE lists columns of persistent views
    When query
      """
      DESCRIBE TABLE customer_view
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | int       | NULL    |
      | name     | string    | NULL    |

  Scenario: DESCRIBE EXTENDED lists columns of persistent views
    When query
      """
      DESCRIBE EXTENDED customer_view
      """
    Then query result row where "col_name" is "id" has "data_type" equal to "int"
    And query result row where "col_name" is "name" has "data_type" equal to "string"

  Scenario: DESCRIBE EXTENDED includes persistent view metadata
    When query
      """
      DESCRIBE EXTENDED customer_view
      """
    Then query result has row where "col_name" is "# Detailed Table Information"
    And query result row where "col_name" is "Type" has "data_type" equal to "VIEW"
