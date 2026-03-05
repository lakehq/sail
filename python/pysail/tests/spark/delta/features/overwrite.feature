Feature: Delta Lake Overwrite

  Rule: Overwrite and conditional overwrite (REPLACE WHERE)
    Background:
      Given variable location for temporary directory delta_overwrite
      Given final statement
        """
        DROP TABLE IF EXISTS delta_overwrite_basic
        """
      Given statement template
        """
        CREATE TABLE delta_overwrite_basic (
          id BIGINT,
          category STRING,
          value BIGINT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        SELECT * FROM VALUES
          (1, 'A', 10),
          (2, 'B', 20),
          (3, 'A', 30),
          (4, 'B', 40)
        AS tab(id, category, value)
        """

    Scenario: EXPLAIN plan for conditional overwrite (REPLACE WHERE category = 'A')
      When query
        """
        EXPLAIN
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE category = 'A'
        SELECT * FROM VALUES
          (5, 'A', 100),
          (6, 'A', 200)
        AS tab(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: Conditional overwrite keeps non-matching rows (REPLACE WHERE)
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE category = 'A'
        SELECT * FROM VALUES
          (5, 'A', 100),
          (6, 'A', 200)
        AS tab(id, category, value)
        """
      Then delta log latest commit info matches snapshot
      When query
        """
        SELECT id, category, value FROM delta_overwrite_basic ORDER BY id
        """
      Then query result ordered
        | id | category | value |
        | 2  | B        | 20    |
        | 4  | B        | 40    |
        | 5  | A        | 100   |
        | 6  | A        | 200   |

    Scenario: EXPLAIN plan for full conditional overwrite (REPLACE WHERE id >= CAST(0 AS BIGINT))
      When query
        """
        EXPLAIN
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE id >= CAST(0 AS BIGINT)
        SELECT * FROM VALUES
          (10, 'C', 999),
          (11, 'D', 111)
        AS tab(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: Conditional overwrite can replace all rows (REPLACE WHERE id >= CAST(0 AS BIGINT))
      Given statement
        """
        INSERT INTO delta_overwrite_basic
        REPLACE WHERE id >= CAST(0 AS BIGINT)
        SELECT * FROM VALUES
          (10, 'C', 999),
          (11, 'D', 111)
        AS tab(id, category, value)
        """
      Then delta log latest commit info matches snapshot
      When query
        """
        SELECT id, category, value FROM delta_overwrite_basic ORDER BY id
        """
      Then query result ordered
        | id | category | value |
        | 10 | C        | 999   |
        | 11 | D        | 111   |

