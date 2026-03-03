Feature: Delta Lake Update

  Rule: Basic operations
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_basic
        """
      Given statement template
        """
        CREATE TABLE delta_update_basic (
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
        INSERT INTO delta_update_basic
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
      Given statement
        """
        UPDATE delta_update_basic SET salary = 99000 WHERE name = 'Alice'
        """
      Then delta log latest commit info contains
        | path      | value    |
        | operation | "UPDATE" |
      When query
        """
        SELECT id, name, salary FROM delta_update_basic WHERE name = 'Alice'
        """
      Then query result
        | id | name  | salary |
        | 1  | Alice | 99000  |

    Scenario: Update multiple columns at once
      Given statement
        """
        UPDATE delta_update_basic
        SET salary = 80000, department = 'Operations'
        WHERE id = 2
        """
      When query
        """
        SELECT id, name, salary, department FROM delta_update_basic WHERE id = 2
        """
      Then query result
        | id | name | salary | department |
        | 2  | Bob  | 80000  | Operations |

    Scenario: Update using column expression
      Given statement
        """
        UPDATE delta_update_basic SET salary = salary + 5000 WHERE department = 'Sales'
        """
      When query
        """
        SELECT id, name, salary FROM delta_update_basic
        WHERE department = 'Sales'
        ORDER BY id
        """
      Then query result ordered
        | id | name  | salary |
        | 4  | Diana | 60000  |
        | 7  | Grace | 55000  |

    Scenario: Condition that matches no rows
      Given statement
        """
        UPDATE delta_update_basic SET salary = 0 WHERE age > 100
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_update_basic WHERE salary = 0
        """
      Then query result
        | count |
        | 0     |

    Scenario: Condition that matches all rows
      Given statement
        """
        UPDATE delta_update_basic SET active = false WHERE age > 0
        """
      When query
        """
        SELECT COUNT(*) AS count FROM delta_update_basic WHERE active = false
        """
      Then query result
        | count |
        | 8     |

    Scenario: Multiple conditions
      Given statement
        """
        UPDATE delta_update_basic
        SET salary = 100000
        WHERE (department = 'Engineering' AND age > 30)
          OR (department = 'Marketing' AND salary < 70000)
        """
      When query
        """
        SELECT id, name, salary FROM delta_update_basic
        WHERE salary = 100000
        ORDER BY id
        """
      Then query result ordered
        | id | name    | salary |
        | 2  | Bob     | 100000 |
        | 3  | Charlie | 100000 |
        | 6  | Frank   | 100000 |

  Rule: Operations on partitioned tables
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_partitioned
        """
      Given statement template
        """
        CREATE TABLE delta_update_partitioned (
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
        INSERT INTO delta_update_partitioned
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
        UPDATE delta_update_partitioned SET value = 0 WHERE year = 2023
        """
      When query
        """
        SELECT id, name, year, month, value
        FROM delta_update_partitioned ORDER BY id
        """
      Then query result ordered
        | id | name    | year | month | value |
        | 1  | Alice   | 2023 | 1     | 0     |
        | 2  | Bob     | 2023 | 1     | 0     |
        | 3  | Charlie | 2023 | 2     | 0     |
        | 4  | Diana   | 2023 | 2     | 0     |
        | 5  | Eve     | 2024 | 1     | 500   |
        | 6  | Frank   | 2024 | 1     | 600   |
        | 7  | Grace   | 2024 | 2     | 700   |
        | 8  | Henry   | 2024 | 2     | 800   |

    Scenario: Non-partition column condition
      Given statement
        """
        UPDATE delta_update_partitioned SET value = value * 2 WHERE value > 500
        """
      When query
        """
        SELECT id, name, year, month, value
        FROM delta_update_partitioned ORDER BY id
        """
      Then query result ordered
        | id | name    | year | month | value |
        | 1  | Alice   | 2023 | 1     | 100   |
        | 2  | Bob     | 2023 | 1     | 200   |
        | 3  | Charlie | 2023 | 2     | 300   |
        | 4  | Diana   | 2023 | 2     | 400   |
        | 5  | Eve     | 2024 | 1     | 500   |
        | 6  | Frank   | 2024 | 1     | 1200  |
        | 7  | Grace   | 2024 | 2     | 1400  |
        | 8  | Henry   | 2024 | 2     | 1600  |

  Rule: Operations for null values
    Background:
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_null
        """
      Given statement template
        """
        CREATE TABLE delta_update_null (
          id INT,
          name STRING,
          department STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_null
        SELECT * FROM VALUES
          (1, 'Alice', 'Engineering'),
          (2, 'Bob', NULL),
          (3, NULL, 'Marketing'),
          (4, 'Diana', 'Sales'),
          (5, NULL, NULL)
        """

    Scenario: Update rows where column is null
      Given statement
        """
        UPDATE delta_update_null SET department = 'Unknown' WHERE department IS NULL
        """
      When query
        """
        SELECT * FROM delta_update_null ORDER BY id
        """
      Then query result ordered
        | id | name  | department  |
        | 1  | Alice | Engineering |
        | 2  | Bob   | Unknown     |
        | 3  | NULL  | Marketing   |
        | 4  | Diana | Sales       |
        | 5  | NULL  | Unknown     |

    Scenario: Set a column to null
      Given statement
        """
        UPDATE delta_update_null SET name = NULL WHERE id = 1
        """
      When query
        """
        SELECT id, name FROM delta_update_null WHERE id = 1
        """
      Then query result
        | id | name |
        | 1  | NULL |
