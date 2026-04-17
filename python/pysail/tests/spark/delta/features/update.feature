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

    Scenario: Simple WHERE condition
      Given statement
        """
        UPDATE delta_update_basic SET salary = 100000 WHERE department = 'Engineering'
        """
      Then delta log latest commit info contains
        | path                          | value                           |
        | operation                     | "UPDATE"                        |
        | operationParameters.predicate | "department = 'Engineering' " |
      When query
        """
        SELECT id, name, salary FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | name    | salary |
        | 1  | Alice   | 100000 |
        | 2  | Bob     | 65000  |
        | 3  | Charlie | 100000 |
        | 4  | Diana   | 55000  |
        | 5  | Eve     | 70000  |
        | 6  | Frank   | 100000 |
        | 7  | Grace   | 50000  |
        | 8  | Henry   | 60000  |

    Scenario: Multi-predicate WHERE
      Given statement
        """
        UPDATE delta_update_basic
        SET active = false
        WHERE (department = 'Marketing' AND age > 30)
          OR (department = 'Sales' AND salary < 60000)
        """
      When query
        """
        SELECT id, name, active FROM delta_update_basic ORDER BY id
        """
      Then query result ordered
        | id | name    | active |
        | 1  | Alice   | true   |
        | 2  | Bob     | true   |
        | 3  | Charlie | false  |
        | 4  | Diana   | false  |
        | 5  | Eve     | false  |
        | 6  | Frank   | false  |
        | 7  | Grace   | false  |
        | 8  | Henry   | true   |

    Scenario: Condition that matches no rows
      Given statement
        """
        UPDATE delta_update_basic SET salary = 0 WHERE age > 100
        """
      When query
        """
        SELECT COUNT(*) as count FROM delta_update_basic WHERE salary = 0
        """
      Then query result
        | count |
        | 0     |

    Scenario: No WHERE updates all rows
      Given statement
        """
        UPDATE delta_update_basic SET active = false
        """
      When query
        """
        SELECT COUNT(*) as count FROM delta_update_basic WHERE active = true
        """
      Then query result
        | count |
        | 0     |

    Scenario: Multi-column SET
      Given statement
        """
        UPDATE delta_update_basic
        SET salary = 99000, active = false
        WHERE id = 1
        """
      When query
        """
        SELECT id, salary, active FROM delta_update_basic WHERE id = 1
        """
      Then query result
        | id | salary | active |
        | 1  | 99000  | false  |

    Scenario: Self-referential RHS
      Given statement
        """
        UPDATE delta_update_basic SET salary = salary + 1000 WHERE department = 'Sales'
        """
      When query
        """
        SELECT id, name, salary FROM delta_update_basic WHERE department = 'Sales' ORDER BY id
        """
      Then query result ordered
        | id | name  | salary |
        | 4  | Diana | 56000  |
        | 7  | Grace | 51000  |

    Scenario: Set to NULL
      Given statement
        """
        UPDATE delta_update_basic SET department = NULL WHERE id = 2
        """
      When query
        """
        SELECT id, department FROM delta_update_basic WHERE id = 2
        """
      Then query result
        | id | department |
        | 2  | NULL       |

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

    Scenario: Partition-only predicate
      Given statement
        """
        UPDATE delta_update_partitioned SET value = 0 WHERE year = 2023
        """
      When query
        """
        SELECT id, name, year, month, value FROM delta_update_partitioned ORDER BY id
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

  Rule: Null handling
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

    Scenario: WHERE col IS NULL
      Given statement
        """
        UPDATE delta_update_null SET department = 'Unknown' WHERE department IS NULL
        """
      When query
        """
        SELECT id, department FROM delta_update_null ORDER BY id
        """
      Then query result ordered
        | id | department  |
        | 1  | Engineering |
        | 2  | Unknown     |
        | 3  | Marketing   |
        | 4  | Sales       |
        | 5  | Unknown     |
