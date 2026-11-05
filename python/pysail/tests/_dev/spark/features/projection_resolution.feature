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

  Scenario Outline: Aggregate projections evaluate each volatile lateral alias once
    When query
      """
      SELECT n, x = y AS same_value, y + n = z AS same_chain, x <> independent AS separate_value
      FROM (
        SELECT count(*) AS n, rand() AS x, x AS y, y + n AS z, rand() AS independent
        FROM range(<rows>)
      ) q
      """
    Then query result collected
      | n      | same_value | same_chain | separate_value |
      | <rows> | true       | true       | true           |

    Examples:
      | rows |
      | 0    |
      | 10   |

  Scenario: Window results can be reused through lateral aliases
    When query
      """
      SELECT row_number() OVER (ORDER BY id) AS x, x + 1 AS y, y + 1 AS z
      FROM range(3)
      """
    Then query result collected
      | x | y | z |
      | 1 | 2 | 3 |
      | 2 | 3 | 4 |
      | 3 | 4 | 5 |

  Scenario Outline: Partition expressions are lowered before materializing lateral aliases
    When query
      """
      SELECT count(*) AS n, bool_and(x = y) AS same_value
      FROM (SELECT <expression> AS x, x AS y FROM range(3)) q
      """
    Then query result collected
      | n | same_value |
      | 3 | true       |

    Examples:
      | expression                    |
      | monotonically_increasing_id() |
      | spark_partition_id()          |

  Scenario: Existence predicates can be reused through lateral aliases
    When query
      """
      SELECT id, EXISTS(SELECT 1 FROM VALUES (1L), (2L) u(k) WHERE u.k = t.id) AS x, x AS y
      FROM range(3) t
      """
    Then query result collected
      | id | x     | y     |
      | 0  | false | false |
      | 1  | true  | true  |
      | 2  | true  | true  |

  Scenario: Lateral aliases provide types for casts and functions
    When query
      """
      SELECT 1 AS a, CAST(a AS STRING) AS b, concat(b, 'x') AS c
      """
    Then query result collected
      | a | b | c  |
      | 1 | 1 | 1x |
    Then query column types
      | column | type   |
      | a      | int    |
      | b      | string |
      | c      | string |

  Scenario Outline: Wildcards expand only the original input with lateral aliases
    When query
      """
      SELECT <projection> FROM VALUES (1) t(v)
      """
    Then query result collected
      | v | x | y |
      | 1 | 2 | 3 |
    Then query column types
      | column | type |
      | v      | int  |
      | x      | int  |
      | y      | int  |

    Examples:
      | projection                  |
      | *, v + 1 AS x, x + 1 AS y   |
      | t.*, v + 1 AS x, x + 1 AS y |

  Scenario: A wildcard after a lateral alias excludes staged columns
    When query
      """
      SELECT v + 1 AS x, *, x + 1 AS y FROM VALUES (1) t(v)
      """
    Then query result collected
      | x | v | y |
      | 2 | 1 | 3 |
    Then query column types
      | column | type |
      | x      | int  |
      | v      | int  |
      | y      | int  |

  Scenario: Aggregate aliases preserve scalar subquery identities
    When query
      """
      SELECT sum((SELECT 1)) AS x, x + 1 AS y FROM range(3)
      """
    Then query result collected
      | x | y |
      | 3 | 4 |

  Scenario: Aggregate aliases retain distinct scalar subquery results
    When query
      """
      SELECT sum((SELECT 1)) AS x, sum((SELECT 2)) AS y, x + y AS z FROM range(3)
      """
    Then query result collected
      | x | y | z |
      | 3 | 6 | 9 |

  Scenario: Grouped aliases retain their types in HAVING
    When query
      """
      SELECT id % 2 AS k, sum(id) AS s, s + 1 AS next, cast(next AS STRING) AS label
      FROM range(4)
      GROUP BY k
      HAVING cast(next AS BIGINT) > 2
      """
    Then query result collected
      | k | s | next | label |
      | 0 | 2 | 3    | 3     |
      | 1 | 4 | 5    | 5     |

  Scenario Outline: Grouped alias chains use materialized values
    When query
      """
      SELECT a0, a16 FROM (
        SELECT id AS a0,
               a0 + a0 AS a1, a1 + a1 AS a2, a2 + a2 AS a3, a3 + a3 AS a4,
               a4 + a4 AS a5, a5 + a5 AS a6, a6 + a6 AS a7, a7 + a7 AS a8,
               a8 + a8 AS a9, a9 + a9 AS a10, a10 + a10 AS a11, a11 + a11 AS a12,
               a12 + a12 AS a13, a13 + a13 AS a14, a14 + a14 AS a15, a15 + a15 AS a16
        FROM range(2)
        GROUP BY <grouping>
        HAVING id >= 0
      ) q
      """
    Then query result collected
      | a0 | a16   |
      | 0  | 0     |
      | 1  | 65536 |

    Examples:
      | grouping               |
      | id                     |
      | ALL                    |
      | a0, cast(a16 AS BIGINT) |

  Scenario: GROUP BY ALL excludes aliases derived from aggregates
    When query
      """
      SELECT sum(id) AS x, x + 1 AS y, y + 1 AS z FROM range(3) GROUP BY ALL
      """
    Then query result collected
      | x | y | z |
      | 3 | 4 | 5 |

  Scenario Outline: Lateral aliases expose nested struct fields
    Given config spark.sql.caseSensitive = <case_sensitive>
    When query
      """
      SELECT named_struct('nested', named_struct('v', 1)) AS s,
             s.<field>.v + 1 AS n,
             array(s) AS items,
             items.<field>.v AS values
      """
    Then query result collected
      | s                   | n | items                 | values |
      | Row(nested=Row(v=1)) | 2 | [Row(nested=Row(v=1))] | [1]    |

    Examples:
      | case_sensitive | field  |
      | false          | NESTED |
      | true           | nested |

  Scenario: Input struct fields take precedence over a lateral alias
    When query
      """
      SELECT named_struct('v', 1) AS s, s.v AS n
      FROM VALUES (named_struct('v', 10)) t(s)
      """
    Then query result collected
      | s        | n  |
      | Row(v=1) | 10 |

  Scenario: An invalid local struct field does not resolve through a lateral alias
    When query
      """
      SELECT named_struct('v', 1) AS s, s.v AS n
      FROM VALUES (named_struct('other', 10)) t(s)
      """
    Then query error (?i)(cannot resolve|unresolved|not found|no such struct field)

  Scenario Outline: Lateral aliases take precedence over outer references
    When query
      """
      SELECT q.x
      FROM VALUES (10) t(a),
        LATERAL (SELECT 1 AS a, a + 1 AS x <grouping>) q
      """
    Then query result collected
      | x |
      | 2 |

    Examples:
      | grouping   |
      |            |
      | GROUP BY 1 |

  Scenario: Select aliases do not shadow outer input columns inside a subquery
    When query
      """
      SELECT 1 AS a, (SELECT 2 WHERE a = 10) AS b
      FROM VALUES (10) t(a)
      """
    Then query result collected
      | a | b |
      | 1 | 2 |
