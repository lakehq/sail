Feature: Semi join support

  Rule: LEFT SEMI JOIN with ON condition

    Scenario: basic left semi join filters matching rows
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW orders AS
        SELECT * FROM VALUES ('Alice', 100), ('Bob', 200), ('Alice', 150), ('Carol', 300) AS t(name, amount)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW vip AS
        SELECT * FROM VALUES ('Alice'), ('Carol') AS t(name)
        """
      When query
        """
        SELECT name, amount FROM orders
        LEFT SEMI JOIN vip ON orders.name = vip.name
        ORDER BY name, amount
        """
      Then query result ordered
        | name  | amount |
        | Alice | 100    |
        | Alice | 150    |
        | Carol | 300    |

    Scenario: left semi join with subquery and aggregation
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW sales AS
        SELECT * FROM VALUES ('A', 10), ('B', 20), ('A', 30), ('C', 5), ('B', 15), ('C', 25) AS t(product, qty)
        """
      When query
        """
        SELECT s.product, s.qty FROM sales s
        LEFT SEMI JOIN (
          SELECT product, SUM(qty) AS total
          FROM sales GROUP BY product
          ORDER BY total DESC LIMIT 2
        ) AS topk ON s.product = topk.product
        ORDER BY s.product, s.qty
        """
      Then query result ordered
        | product | qty |
        | A       | 10  |
        | A       | 30  |
        | B       | 15  |
        | B       | 20  |

  Rule: LEFT ANTI JOIN with ON condition

    Scenario: left anti join excludes matching rows
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW orders AS
        SELECT * FROM VALUES ('Alice', 100), ('Bob', 200), ('Carol', 300) AS t(name, amount)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW vip AS
        SELECT * FROM VALUES ('Alice'), ('Carol') AS t(name)
        """
      When query
        """
        SELECT name, amount FROM orders
        LEFT ANTI JOIN vip ON orders.name = vip.name
        ORDER BY name
        """
      Then query result ordered
        | name | amount |
        | Bob  | 200    |
