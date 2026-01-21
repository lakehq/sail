@sail_env_SAIL_MODE__local-cluster
Feature: Distributed Execution (local-cluster)

  Scenario: Basic query execution in cluster mode
    When query
      """
      SELECT 1 + 1 AS result
      """
    Then query result ordered
      | result |
      | 2      |

  Scenario: Basic DataFrame-like operations in cluster mode
    Given final statement
      """
      DROP VIEW IF EXISTS exec_df_ops
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW exec_df_ops AS
      SELECT * FROM VALUES
        (1, 'hello'),
        (2, 'world'),
        (3, 'test')
      AS t(a, b)
      """
    When query
      """
      SELECT a, b
      FROM exec_df_ops
      WHERE a > 1
      ORDER BY a
      """
    Then query result ordered
      | a | b     |
      | 2 | world |
      | 3 | test  |

  Scenario: Aggregation with groupby is correct in cluster mode
    Given final statement
      """
      DROP VIEW IF EXISTS exec_large_dataset
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW exec_large_dataset AS
      SELECT
        id,
        CAST(id % 10 AS INT) AS grp,
        (id * 2) AS value
      FROM range(0, 1000)
      """
    When query
      """
      SELECT
        grp,
        COUNT(*) AS count,
        SUM(value) AS sum_value,
        CAST(AVG(value) AS BIGINT) AS avg_value,
        MAX(value) AS max_value,
        MIN(value) AS min_value
      FROM exec_large_dataset
      GROUP BY grp
      ORDER BY grp
      """
    Then query result ordered
      | grp | count | sum_value | avg_value | max_value | min_value |
      | 0   | 100   | 99000     | 990       | 1980      | 0         |
      | 1   | 100   | 99200     | 992       | 1982      | 2         |
      | 2   | 100   | 99400     | 994       | 1984      | 4         |
      | 3   | 100   | 99600     | 996       | 1986      | 6         |
      | 4   | 100   | 99800     | 998       | 1988      | 8         |
      | 5   | 100   | 100000    | 1000      | 1990      | 10        |
      | 6   | 100   | 100200    | 1002      | 1992      | 12        |
      | 7   | 100   | 100400    | 1004      | 1994      | 14        |
      | 8   | 100   | 100600    | 1006      | 1996      | 16        |
      | 9   | 100   | 100800    | 1008      | 1998      | 18        |

  Scenario: Join operations are correct in cluster mode
    Given final statement
      """
      DROP VIEW IF EXISTS exec_customers
      """
    Given final statement
      """
      DROP VIEW IF EXISTS exec_orders
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW exec_customers AS
      SELECT * FROM VALUES
        (1, 'Alice', 'NYC'),
        (2, 'Bob', 'LA'),
        (3, 'Charlie', 'Chicago')
      AS t(id, name, city)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW exec_orders AS
      SELECT * FROM VALUES
        (1, 101, 100.0),
        (1, 102, 150.0),
        (2, 103, 200.0),
        (3, 104, 75.0)
      AS t(customer_id, order_id, amount)
      """
    When query
      """
      SELECT
        c.name,
        c.city,
        o.order_id,
        o.amount
      FROM exec_customers c
      JOIN exec_orders o ON c.id = o.customer_id
      ORDER BY o.order_id
      """
    Then query result ordered
      | name    | city    | order_id | amount |
      | Alice   | NYC     | 101      | 100.0  |
      | Alice   | NYC     | 102      | 150.0  |
      | Bob     | LA      | 103      | 200.0  |
      | Charlie | Chicago | 104      | 75.0   |

  Scenario: Complex SQL query returns expected shape in cluster mode
    Given final statement
      """
      DROP VIEW IF EXISTS exec_sales
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW exec_sales AS
      SELECT
        id AS sale_id,
        CAST(id % 20 AS INT) AS product_id,
        concat('region_', CAST(id % 5 AS STRING)) AS region,
        CAST(100 + (id % 100) AS INT) AS amount,
        concat('2024-', lpad(CAST((id % 12) + 1 AS STRING), 2, '0'), '-01') AS sale_date
      FROM range(0, 500)
      """
    When query
      """
      WITH regional_sales AS (
        SELECT
          region,
          product_id,
          SUM(amount) AS total_amount,
          COUNT(*) AS sale_count,
          AVG(amount) AS avg_amount
        FROM exec_sales
        GROUP BY region, product_id
      ),
      top_products AS (
        SELECT
          region,
          product_id,
          total_amount,
          ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_amount DESC) AS rank
        FROM regional_sales
      )
      SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT region) AS regions,
        MAX(rank) AS max_rank
      FROM top_products
      WHERE rank <= 3
      """
    Then query result ordered
      | rows | regions | max_rank |
      | 15   | 5       | 3        |

  Scenario: Multiple operations succeed in cluster mode
    When query
      """
      WITH df1 AS (SELECT id AS id1 FROM range(0, 100)),
           df2 AS (SELECT id AS id2 FROM range(0, 100))
      SELECT
        (SELECT SUM(id1) FROM df1) AS sum1,
        (SELECT SUM(id2) FROM df2) AS sum2,
        (SELECT COUNT(*) FROM df1 JOIN df2 ON df1.id1 = df2.id2) AS join_count
      """
    Then query result ordered
      | sum1 | sum2 | join_count |
      | 4950 | 4950 | 100        |

