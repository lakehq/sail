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

  Rule: Projected uncorrelated IN subquery

    Scenario: projected IN preserves input rows and ignores duplicate matches
      When query
        """
        SELECT id, id IN (SELECT x FROM VALUES (1), (1), (2) t(x)) AS present
        FROM VALUES (1), (2), (3), (3) u(id)
        ORDER BY id
        """
      Then query result ordered
        | id | present |
        | 1  | true    |
        | 2  | true    |
        | 3  | false   |
        | 3  | false   |
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- present: boolean (nullable = false)
        """

    Scenario: projected IN and NOT IN account for null candidates and nested negation
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS present,
          id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS absent,
          NOT (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) AS negated,
          NOT (id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x))) AS double_negated,
          NOT (NOT (id IN (SELECT x FROM VALUES (1), (NULL) t(x)))) AS nested_not
        FROM VALUES (1), (2), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent | negated | double_negated | nested_not |
        | 1    | true    | false  | false   | true           | true       |
        | 2    | false   | false  | false   | false          | false      |
        | NULL | false   | false  | false   | false          | false      |
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
         |-- present: boolean (nullable = true)
         |-- absent: boolean (nullable = true)
         |-- negated: boolean (nullable = true)
         |-- double_negated: boolean (nullable = true)
         |-- nested_not: boolean (nullable = true)
        """

    Scenario: projected NOT IN rejects null input against a nonempty subquery
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1), (2) t(x)) AS present,
          id NOT IN (SELECT x FROM VALUES (1), (2) t(x)) AS absent
        FROM VALUES (1), (3), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | true    | false  |
        | 3    | false   | true   |
        | NULL | false   | false  |

    Scenario: projected IN and NOT IN use empty subquery semantics even for null input
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS present,
          id NOT IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS absent
        FROM VALUES (1), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | false   | true   |
        | NULL | false   | true   |

    Scenario: projected IN supports expressions with comparison coercion inside CASE
      When query
        """
        SELECT id,
          CASE WHEN id + 1 IN (SELECT CAST(x AS BIGINT) FROM VALUES (2), (4) t(x))
            THEN 'hit' ELSE 'miss' END AS label
        FROM VALUES (1), (2), (3) u(id)
        ORDER BY id
        """
      Then query result ordered
        | id | label |
        | 1  | hit   |
        | 2  | miss  |
        | 3  | hit   |

    Scenario: projected IN compares aggregate results
      When query
        """
        SELECT
          SUM(id) IN (SELECT MAX(x) FROM VALUES (2), (3) t(x)) AS present,
          SUM(id) NOT IN (SELECT MAX(x) FROM VALUES (2) t(x)) AS absent
        FROM VALUES (1), (2) u(id)
        """
      Then query result
        | present | absent |
        | true    | true   |

    Scenario: projected IN keeps a scalar aggregate row from an empty input
      When query
        """
        SELECT id,
          id IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS present,
          id NOT IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS absent
        FROM VALUES (1), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | false   | false  |
        | NULL | false   | false  |

    @sail-bug
    Scenario Outline: projected IN normalizes indirect negation before decorrelation
      # TODO: Preserve Spark's optimizer ordering for wrappers that become NOT IN.
      When query
        """
        SELECT id, <predicate> AS absent
        FROM VALUES (1), (2), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | absent |
        | 1    | false  |
        | 2    | false  |
        | NULL | false  |

      Examples:
        | predicate                                                          |
        | NOT CAST(id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS BOOLEAN) |
        | (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) = FALSE            |

  Rule: Struct constructors supply IN subquery values

    Scenario: a single-field struct supplies one IN subquery value
      When query
        """
        WITH structs AS (
          SELECT STRUCT(x AS x, y AS y) AS s
          FROM VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (2, 3) t(x, y)
        )
        SELECT
          STRUCT(STRUCT(1 AS x, 2 AS y) AS s) IN (SELECT s FROM structs) AS present,
          STRUCT(STRUCT(9 AS x, 9 AS y) AS s) IN (SELECT s FROM structs) AS missing
        """
      Then query result
        | present | missing |
        | true    | false   |

    Scenario: a single-field struct supplies one filtered IN subquery value
      When query
        """
        SELECT id FROM VALUES (1), (2), (3) t(id)
        WHERE STRUCT(id) IN (SELECT id FROM VALUES (1), (3) u(id))
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |
