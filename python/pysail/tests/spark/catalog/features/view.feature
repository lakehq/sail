Feature: Persistent views

  Background:
    Given variable customers_table for random identifier with prefix view_customers_
    Given variable orders_table for random identifier with prefix view_orders_
    Given final statement template
      """
      DROP TABLE IF EXISTS {{ customers_table }}
      """
    Given final statement
      """
      DROP VIEW IF EXISTS customer_view
      """
    Given statement template
      """
      CREATE TABLE {{ customers_table }} (id INT, name STRING)
      """
    Given statement template
      """
      INSERT INTO {{ customers_table }} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')
      """
    Given statement template
      """
      CREATE VIEW customer_view AS SELECT * FROM {{ customers_table }}
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
    Given statement template
      """
      CREATE VIEW active_customers AS SELECT * FROM {{ customers_table }} WHERE id = 1
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
    Given final statement template
      """
      DROP TABLE IF EXISTS {{ orders_table }}
      """
    Given statement template
      """
      CREATE VIEW active_customers AS SELECT * FROM {{ customers_table }} WHERE id = 1
      """
    Given statement template
      """
      CREATE TABLE {{ orders_table }} (order_id INT, customer_id INT)
      """
    Given statement template
      """
      INSERT INTO {{ orders_table }} VALUES (100, 1), (101, 2)
      """
    When query template
      """
      SELECT active_customers.name, {{ orders_table }}.order_id
      FROM active_customers
      JOIN {{ orders_table }} ON active_customers.id = {{ orders_table }}.customer_id
      ORDER BY {{ orders_table }}.order_id
      """
    Then query result ordered
      | name  | order_id |
      | Alice | 100      |

  Scenario: Create persistent view with column aliases
    Given final statement
      """
      DROP VIEW IF EXISTS aliased_customers
      """
    Given statement template
      """
      CREATE VIEW aliased_customers (customer_id, customer_name) AS SELECT id, name FROM {{ customers_table }}
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
    Then query result has row where "tableName" is "{{ customers_table }}"
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
