Feature: Projection and alias resolution
  Background:
    Given config spark.sql.caseSensitive = false
    Given config spark.sql.ansi.enabled = true
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW t AS
      SELECT * FROM VALUES
        (1L, 'a', 10, 100),
        (2L, 'a', 20, 200),
        (3L, 'b', CAST(NULL AS INT), 300),
        (4L, 'b', 40, CAST(NULL AS INT)),
        (5L, CAST(NULL AS STRING), 50, 500),
        (6L, 'a', 10, 600),
        (7L, 'c', 70, 700)
      AS t(id, g, v, w)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS t
      """

  Scenario: Select aliases can refer to earlier aliases in a chain
    When query
      """
      SELECT v + 1 AS x, x + 1 AS y, y + 1 AS z
      FROM t
      """
    Then query result collected
      | x    | y    | z    |
      | 11   | 12   | 13   |
      | 11   | 12   | 13   |
      | 21   | 22   | 23   |
      | 41   | 42   | 43   |
      | 51   | 52   | 53   |
      | 71   | 72   | 73   |
      | NULL | NULL | NULL |
    Then query column types
      | column | type |
      | x      | int  |
      | y      | int  |
      | z      | int  |

  Scenario: Star EXCEPT removes only the named column
    When query
      """
      SELECT * EXCEPT (w)
      FROM t
      """
    Then query result collected
      | id | g    | v    |
      | 1  | a    | 10   |
      | 2  | a    | 20   |
      | 3  | b    | NULL |
      | 4  | b    | 40   |
      | 5  | NULL | 50   |
      | 6  | a    | 10   |
      | 7  | c    | 70   |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |

  Scenario: Qualified stars resolve aliases without case sensitivity
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT X.*
      FROM t x
      """
    Then query result collected
      | id | g    | v    | w    |
      | 1  | a    | 10   | 100  |
      | 2  | a    | 20   | 200  |
      | 3  | b    | NULL | 300  |
      | 4  | b    | 40   | NULL |
      | 5  | NULL | 50   | 500  |
      | 6  | a    | 10   | 600  |
      | 7  | c    | 70   | 700  |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |

  Scenario: Select aliases resolve earlier names without case sensitivity
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT v AS Foo, foo + 1 AS x
      FROM t
      """
    Then query result collected
      | Foo  | x    |
      | 10   | 11   |
      | 10   | 11   |
      | 20   | 21   |
      | 40   | 41   |
      | 50   | 51   |
      | 70   | 71   |
      | NULL | NULL |
    Then query column types
      | column | type |
      | Foo    | int  |
      | x      | int  |

  Scenario: An aggregate alias can be used by a later select expression
    When query
      """
      SELECT g, sum(v) AS s, s + 1 AS x
      FROM t
      GROUP BY g
      """
    Then query result collected
      | g    | s  | x  |
      | a    | 40 | 41 |
      | b    | 40 | 41 |
      | c    | 70 | 71 |
      | NULL | 50 | 51 |
    Then query column types
      | column | type   |
      | g      | string |
      | s      | bigint |
      | x      | bigint |

  Scenario: A qualified star resolves a differently cased subquery alias
    When query
      """
      SELECT X.*
      FROM (SELECT 1 AS a) x
      """
    Then query result collected
      | a |
      | 1 |
    Then query column types
      | column | type |
      | a      | int  |

  Scenario Outline: Input columns take precedence over materialized lateral aliases
    When query
      """
      SELECT <expression> AS V, 1 AS a, a + v AS b, b + a AS c
      FROM VALUES (10) source(v)
      """
    Then query result collected
      | V        | a | b  | c  |
      | <value>  | 1 | 11 | 12 |

    Examples:
      | expression | value |
      | v          | 10    |
      | v + 100    | 110   |

  Scenario: A qualified star includes the USING key in a SQL join
    When query
      """
      SELECT t.*
      FROM VALUES (1, 'a'), (2, 'b') AS t(id, l)
      JOIN VALUES (1, 'x') AS s(id, r) USING (id)
      """
    Then query result collected
      | id | l |
      | 1  | a |
    Then query column types
      | column | type   |
      | id     | int    |
      | l      | string |

  Scenario: A later projection references an alias without a FROM clause
    When query
      """
      SELECT 1 AS a, a + 1 AS b
      """
    Then query result collected
      | a | b |
      | 1 | 2 |
    Then query column types
      | column | type |
      | a      | int  |
      | b      | int  |
