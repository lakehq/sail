Feature: Delta Lake Delete

  Rule: Basic operations
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_delete_basic
        """
      Given statement template
        """
        CREATE TABLE delta_delete_basic (
          id INT,
          name STRING,
          age INT,
          department STRING,
          salary INT,
          active BOOLEAN
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_delete_basic
        SELECT * FROM VALUES
          (1, 'Alice', 25, 'Engineering', 75000, true),
          (2, 'Bob', 30, 'Marketing', 65000, true),
          (3, 'Charlie', 35, 'Engineering', 85000, false),
          (4, 'Diana', 28, 'Sales', 55000, true),
          (5, 'Eve', 32, 'Marketing', 70000, true),
          (6, 'Frank', 40, 'Engineering', 95000, false),
          (7, 'Grace', 27, 'Sales', 50000, true),
          (8, 'Henry', 33, 'HR', 60000, true)
        """

    Scenario: Simple condition
      When query
        """
        SELECT COUNT(*) as count FROM delta_delete_basic
        """
      Then query result
        | count |
        | 8     |
      Given statement
        """
        DELETE FROM delta_delete_basic WHERE department = 'Engineering'
        """
      Then delta log latest commit info matches snapshot
      When query
        """
        SELECT * FROM delta_delete_basic
        """
      Then query result
        | id | name  | age | department | salary | active |
        | 2  | Bob   | 30  | Marketing  | 65000  | true   |
        | 4  | Diana | 28  | Sales      | 55000  | true   |
        | 5  | Eve   | 32  | Marketing  | 70000  | true   |
        | 7  | Grace | 27  | Sales      | 50000  | true   |
        | 8  | Henry | 33  | HR         | 60000  | true   |

    Scenario: Multiple conditions
      Given statement
        """
        DELETE FROM delta_delete_basic
        WHERE (department = 'Marketing' AND age > 30)
          OR (department = 'Sales' AND salary < 60000)
        """
      When query
        """
        SELECT * FROM delta_delete_basic ORDER BY id
        """
      Then query result ordered
        | id | name    | age | department  | salary | active |
        | 1  | Alice   | 25  | Engineering | 75000  | true   |
        | 2  | Bob     | 30  | Marketing   | 65000  | true   |
        | 3  | Charlie | 35  | Engineering | 85000  | false  |
        | 6  | Frank   | 40  | Engineering | 95000  | false  |
        | 8  | Henry   | 33  | HR          | 60000  | true   |

    Scenario: Condition that matches no rows
      Given statement
        """
        DELETE FROM delta_delete_basic WHERE age > 100
        """
      When query
        """
        SELECT COUNT(*) as count FROM delta_delete_basic
        """
      Then query result
        | count |
        | 8     |

    Scenario: Condition that matches all rows
      Given statement
        """
        DELETE FROM delta_delete_basic WHERE age > 0
        """
      When query
        """
        SELECT COUNT(*) as count FROM delta_delete_basic
        """
      Then query result
        | count |
        | 0     |

    Scenario: Complex conditions
      Given statement
      """
      DELETE FROM delta_delete_basic WHERE salary > 69375
      """
      When query
      """
      SELECT * FROM delta_delete_basic ORDER BY salary
      """
      Then query result
        | id | name  | age | department | salary | active |
        | 7  | Grace | 27  | Sales      | 50000  | true   |
        | 4  | Diana | 28  | Sales      | 55000  | true   |
        | 8  | Henry | 33  | HR         | 60000  | true   |
        | 2  | Bob   | 30  | Marketing  | 65000  | true   |

  Rule: Operations on partitioned tables
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_delete_partitioned
        """
      Given statement template
        """
        CREATE TABLE delta_delete_partitioned (
          id INT,
          name STRING,
          year INT,
          month INT,
          value INT
        )
        USING DELTA LOCATION {{ location.sql }}
        PARTITIONED BY (year, month)
        """
      Given statement
        """
        INSERT INTO delta_delete_partitioned
        SELECT * FROM VALUES
          (1, 'Alice', 2023, 1, 100),
          (2, 'Bob', 2023, 1, 200),
          (3, 'Charlie', 2023, 2, 300),
          (4, 'Diana', 2023, 2, 400),
          (5, 'Eve', 2024, 1, 500),
          (6, 'Frank', 2024, 1, 600),
          (7, 'Grace', 2024, 2, 700),
          (8, 'Henry', 2024, 2, 800)
        """

    Scenario: Partition column condition
      Given statement
        """
        DELETE FROM delta_delete_partitioned WHERE year = 2023
        """
      When query
        """
        SELECT id, name, year, month, value
        FROM delta_delete_partitioned ORDER BY id
        """
      Then query result
        | id | name  | year | month | value |
        | 5  | Eve   | 2024 | 1     | 500   |
        | 6  | Frank | 2024 | 1     | 600   |
        | 7  | Grace | 2024 | 2     | 700   |
        | 8  | Henry | 2024 | 2     | 800   |

  Rule: Operations with string comparisons
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_delete_case
        """
      Given statement template
        """
        CREATE TABLE delta_delete_case (
          id INT,
          name STRING,
          department STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_delete_case
        SELECT * FROM VALUES
          (1, 'Alice', 'Engineering'),
          (2, 'alice', 'engineering'),
          (3, 'ALICE', 'ENGINEERING'),
          (4, 'Bob', 'Marketing')
        """

    Scenario: Case-sensitive string comparison
      Given statement
        """
        DELETE FROM delta_delete_case WHERE name = 'Alice'
        """
      When query
        """
        SELECT * FROM delta_delete_case ORDER BY id
        """
      Then query result
        | id | name  | department  |
        | 2  | alice | engineering |
        | 3  | ALICE | ENGINEERING |
        | 4  | Bob   | Marketing   |

  Rule: Operations for null values
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_delete_null
        """
      Given statement template
        """
        CREATE TABLE delta_delete_null (
          id INT,
          name STRING,
          department STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_delete_null
        SELECT * FROM VALUES
          (1, 'Alice', 'Engineering'),
          (2, 'Bob', NULL),
          (3, NULL, 'Marketing'),
          (4, 'Diana', 'Sales'),
          (5, NULL, NULL)
        """

    Scenario: Null values
      Given statement
        """
        DELETE FROM delta_delete_null WHERE department IS NULL
        """
      When query
        """
        SELECT * FROM delta_delete_null ORDER BY id
        """
      Then query result ordered
        | id | name  | department  |
        | 1  | Alice | Engineering |
        | 3  | NULL  | Marketing   |
        | 4  | Diana | Sales       |

  Rule: EXPLAIN CODEGEN shows stepwise optimization for DELETE
    Background:
      Given variable location for temporary directory delete_explain_codegen
      Given final statement
        """
        DROP TABLE IF EXISTS delta_delete_explain_codegen
        """
      Given statement template
        """
        CREATE TABLE delta_delete_explain_codegen (
          id INT,
          name STRING,
          age INT,
          department STRING,
          salary INT,
          active BOOLEAN
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_delete_explain_codegen
        SELECT * FROM VALUES
          (1, 'Alice', 25, 'Engineering', 75000, true),
          (2, 'Bob', 30, 'Marketing', 65000, true),
          (3, 'Charlie', 35, 'Engineering', 85000, false),
          (4, 'Diana', 28, 'Sales', 55000, true),
          (5, 'Eve', 32, 'Marketing', 70000, true),
          (6, 'Frank', 40, 'Engineering', 95000, false),
          (7, 'Grace', 27, 'Sales', 50000, true),
          (8, 'Henry', 33, 'HR', 60000, true)
        """

    Scenario: EXPLAIN CODEGEN includes plan steps and delta rewrite artifacts for DELETE
      When query
        """
        EXPLAIN CODEGEN
        DELETE FROM delta_delete_explain_codegen
        WHERE salary + 1 > 70000
          AND (department = 'Engineering' OR department = 'Marketing')
          AND active IS NOT NULL
        """
      Then query plan matches snapshot
