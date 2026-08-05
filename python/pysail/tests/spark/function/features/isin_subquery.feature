Feature: IN subquery support

  Rule: Single-column IN subquery

    Scenario: basic IN subquery with range
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age IN (SELECT id FROM range(6)) ORDER BY age
        """
      Then query result ordered
        | age | name  |
        | 2   | Alice |
        | 5   | Bob   |

    Scenario: IN subquery with no matches
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (10, 'Alice'), (20, 'Bob') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age IN (SELECT id FROM range(3))
        """
      Then query result
        | age | name |

    Scenario: NOT IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age NOT IN (SELECT id FROM range(6)) ORDER BY age
        """
      Then query result ordered
        | age | name |
        | 8   | Mike |

    Scenario: IN subquery with values
      When query
        """
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(x)
        WHERE x IN (SELECT * FROM VALUES (2), (4) AS s(y))
        ORDER BY x
        """
      Then query result ordered
        | x |
        | 2 |
        | 4 |

  Rule: Multi-column IN subquery

    Scenario: multi-column IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW lookup AS
        SELECT * FROM VALUES (2, 'Alice'), (8, 'Mike') AS t(id, label)
        """
      When query
        """
        SELECT age, name FROM people
        WHERE (age, name) IN (SELECT id, label FROM lookup)
        ORDER BY age
        """
      Then query result ordered
        | age | name  |
        | 2   | Alice |
        | 8   | Mike  |

    Scenario: multi-column IN subquery with expressions
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(a, b)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW targets AS
        SELECT * FROM VALUES (3, 19), (4, 29) AS t(x, y)
        """
      When query
        """
        SELECT a, b FROM data
        WHERE (a + 1, b - 1) IN (SELECT x, y FROM targets)
        ORDER BY a
        """
      Then query result ordered
        | a | b  |
        | 2 | 20 |
        | 3 | 30 |

    Scenario: multi-column NOT IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW lookup AS
        SELECT * FROM VALUES (2, 'Alice'), (8, 'Mike') AS t(id, label)
        """
      When query
        """
        SELECT age, name FROM people
        WHERE (age, name) NOT IN (SELECT id, label FROM lookup)
        ORDER BY age
        """
      Then query result ordered
        | age | name |
        | 5   | Bob  |
