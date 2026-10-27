Feature: Correlated query results
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
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW u AS SELECT *
      FROM VALUES (1L, 'a', 100), (2L, 'a', 200), (4L, 'b', 400), (8L, 'z', 800), (CAST(NULL AS BIGINT), 'n',
        900) AS u(id, g, z)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS u
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW n AS SELECT id, named_struct('Upper', v, 'lower', w) AS s, array(v, w)
        AS arr, map('x', v, 'y', w) AS m, array(named_struct('x', v, 'y', w)) AS structs
      FROM t
      """
    Given final statement
      """
      DROP VIEW IF EXISTS n
      """

  Scenario: Correlated struct fields resolve without case sensitivity
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT id, (SELECT max(u.z) FROM u WHERE u.id = t.id AND s.Upper > 0) AS z
      FROM n t
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: Correlated struct fields resolve with case sensitivity
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT id, (SELECT max(u.z) FROM u WHERE u.id = t.id AND s.Upper > 0) AS z
      FROM n t
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A scalar subquery rejects duplicate empty grouping sets
    When query
      """
      SELECT id, (SELECT count(*) FROM u WHERE u.id = t.id GROUP BY GROUPING SETS ((), ())) AS n
      FROM t
      """
    Then query error (?i)(more than one row|too_many_rows|grouping sets)

  Scenario: A scalar aggregate supports a correlated inequality
    When query
      """
      SELECT id, (SELECT max(z) FROM u WHERE u.id < t.id) AS z
      FROM t
      """
    Then query result collected
      | id | z    |
      | 1  | NULL |
      | 2  | 100  |
      | 3  | 200  |
      | 4  | 200  |
      | 5  | 400  |
      | 6  | 400  |
      | 7  | 400  |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A scalar aggregate compares an inner value with an outer struct field
    When query
      """
      SELECT id, (SELECT max(z) FROM u WHERE u.z < n.s.Upper) AS z
      FROM n
      """
    Then query result collected
      | id | z    |
      | 1  | NULL |
      | 2  | NULL |
      | 3  | NULL |
      | 4  | NULL |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: Projected IN returns false for unmatched values in a nullable subquery
    When query
      """
      SELECT id, id IN (SELECT id FROM u) AS b
      FROM t
      """
    Then query result collected
      | id | b     |
      | 1  | true  |
      | 2  | true  |
      | 3  | false |
      | 4  | true  |
      | 5  | false |
      | 6  | false |
      | 7  | false |
    Then query column types
      | column | type    |
      | id     | bigint  |
      | b      | boolean |

  Scenario: Projected NOT IN returns true for unmatched values in a nullable subquery
    When query
      """
      SELECT id, id NOT IN (SELECT id FROM u) AS b
      FROM t
      """
    Then query result collected
      | id | b     |
      | 1  | false |
      | 2  | false |
      | 3  | false |
      | 4  | false |
      | 5  | false |
      | 6  | false |
      | 7  | false |
    Then query column types
      | column | type    |
      | id     | bigint  |
      | b      | boolean |

  Scenario: Projected tuple NOT IN compares all key fields
    When query
      """
      SELECT id, (id, g) NOT IN (SELECT id, g FROM u) AS b
      FROM t
      """
    Then query result collected
      | id | b     |
      | 1  | false |
      | 2  | false |
      | 3  | true  |
      | 4  | false |
      | 5  | false |
      | 6  | true  |
      | 7  | true  |
    Then query column types
      | column | type    |
      | id     | bigint  |
      | b      | boolean |

  Scenario: A scalar subquery applies an ordered limit for each outer key
    When query
      """
      SELECT id, (SELECT z FROM u WHERE u.id = t.id ORDER BY z LIMIT 1) AS z
      FROM t
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A left lateral join retains unmatched outer rows
    When query
      """
      SELECT t.id, q.z
      FROM t LEFT JOIN LATERAL (SELECT z FROM u WHERE u.id = t.id) q ON true
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A lateral count returns zero for unmatched outer keys
    When query
      """
      SELECT t.id, q.n
      FROM t, LATERAL (SELECT count(*) AS n FROM u WHERE u.id = t.id) q
      """
    Then query result collected
      | id | n |
      | 1  | 1 |
      | 2  | 1 |
      | 3  | 0 |
      | 4  | 1 |
      | 5  | 0 |
      | 6  | 0 |
      | 7  | 0 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | n      | bigint |

  Scenario: An inner lateral join applies an ordered limit for each outer key
    When query
      """
      SELECT t.id, q.z
      FROM t, LATERAL (SELECT z FROM u WHERE u.id = t.id ORDER BY z LIMIT 1) q
      """
    Then query result collected
      | id | z   |
      | 1  | 100 |
      | 2  | 200 |
      | 4  | 400 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: Nested lateral queries resolve an outer column
    When query
      """
      SELECT t.id, q.x
      FROM t, LATERAL (SELECT r.x FROM LATERAL (SELECT t.v AS x) r) q
      """
    Then query result collected
      | id | x    |
      | 1  | 10   |
      | 2  | 20   |
      | 3  | NULL |
      | 4  | 40   |
      | 5  | 50   |
      | 6  | 10   |
      | 7  | 70   |
    Then query column types
      | column | type   |
      | id     | bigint |
      | x      | int    |

  Scenario: Tuple NOT IN can filter rows using all key fields
    When query
      """
      SELECT id
      FROM t
      WHERE (id, g) NOT IN (SELECT id, g FROM u)
      """
    Then query result collected
      | id |
      | 3  |
      | 6  |
      | 7  |
    Then query column types
      | column | type   |
      | id     | bigint |

  Scenario: Tuple NOT IN is unknown when neither field disproves a match
    When query
      """
      SELECT (1, CAST(NULL AS INT)) NOT IN (SELECT * FROM VALUES (CAST(NULL AS INT), 2)) AS b
      """
    Then query result collected
      | b     |
      | false |
    Then query column types
      | column | type    |
      | b      | boolean |

  Scenario: A scalar subquery rejects duplicate nonempty grouping sets
    When query
      """
      SELECT id, (SELECT sum(z) FROM u WHERE u.id = t.id GROUP BY GROUPING SETS ((id), (id))) AS n
      FROM t
      """
    Then query error (?i)(more than one row|too_many_rows|grouping sets)

  Scenario: A left lateral limit preserves outer keys with no matches
    When query
      """
      SELECT t.id, q.z
      FROM t LEFT JOIN LATERAL (SELECT z FROM u WHERE u.id = t.id ORDER BY z LIMIT 1) q ON true
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: EXISTS can correlate through an unprojected outer struct field
    When query
      """
      SELECT id
      FROM n
      WHERE EXISTS (SELECT 1 FROM u WHERE u.id = n.id AND n.s.Upper > 0)
      """
    Then query result collected
      | id |
      | 1  |
      | 2  |
      | 4  |
    Then query column types
      | column | type   |
      | id     | bigint |

  Scenario: A scalar aggregate can correlate through an outer map lookup
    When query
      """
      SELECT id, (SELECT max(z) FROM u WHERE u.id = n.id AND n.m['x'] > 0) AS z
      FROM n
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A correlated scalar query can return a single nonaggregate value
    When query
      """
      SELECT id, (SELECT z FROM u WHERE u.id = t.id) AS z
      FROM t
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A correlated scalar LIMIT zero returns null for every outer row
    When query
      """
      SELECT id, (SELECT z FROM u WHERE u.id = t.id LIMIT 0) AS z
      FROM t
      """
    Then query result collected
      | id | z    |
      | 1  | NULL |
      | 2  | NULL |
      | 3  | NULL |
      | 4  | NULL |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A lateral query can correlate through a join
    When query
      """
      SELECT t.id, q.z
      FROM t, LATERAL (SELECT u.z FROM u JOIN t x ON u.id = x.id WHERE x.id = t.id) q
      """
    Then query result collected
      | id | z   |
      | 1  | 100 |
      | 2  | 200 |
      | 4  | 400 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: Nested EXISTS resolves each enclosing query scope
    When query
      """
      SELECT id
      FROM t
      WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id AND EXISTS (SELECT 1 FROM t x WHERE x.id = u.id))
      """
    Then query result collected
      | id |
      | 1  |
      | 2  |
      | 4  |
    Then query column types
      | column | type   |
      | id     | bigint |

  Scenario: A lateral scalar expression resolves an outer column
    When query
      """
      SELECT t.id, q.x
      FROM t, LATERAL (SELECT t.v + 1 AS x) q
      """
    Then query result collected
      | id | x    |
      | 1  | 11   |
      | 2  | 21   |
      | 3  | NULL |
      | 4  | 41   |
      | 5  | 51   |
      | 6  | 11   |
      | 7  | 71   |
    Then query column types
      | column | type   |
      | id     | bigint |
      | x      | int    |

  Scenario: A lateral projection can filter and sort by hidden outer columns
    When query
      """
      SELECT q.x
      FROM t, LATERAL (SELECT t.v + 1 AS x) q
      WHERE t.w > 100
      ORDER BY t.id
      """
    Then query result collected ordered
      | x    |
      | 21   |
      | NULL |
      | 51   |
      | 11   |
      | 71   |
    Then query column types
      | column | type |
      | x      | int  |

  Scenario: Nested scalar aggregates resolve enclosing input keys
    When query
      """
      SELECT id, (SELECT max(z) FROM u WHERE u.id = t.id AND u.z > (SELECT max(v) FROM t x WHERE x.id = u.id))
        AS z
      FROM t
      """
    Then query result collected
      | id | z    |
      | 1  | 100  |
      | 2  | 200  |
      | 3  | NULL |
      | 4  | 400  |
      | 5  | NULL |
      | 6  | NULL |
      | 7  | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | z      | int    |

  Scenario: A lateral projection can group and filter by a hidden outer column
    When query
      """
      SELECT q.x, count(*) AS n
      FROM t, LATERAL (SELECT t.v AS x) q
      GROUP BY q.x
      HAVING sum(t.w) > 100
      """
    Then query result collected
      | x    | n |
      | 10   | 2 |
      | 20   | 1 |
      | 50   | 1 |
      | 70   | 1 |
      | NULL | 1 |
    Then query column types
      | column | type   |
      | x      | int    |
      | n      | bigint |

  Scenario: Correlated inequality counts preserve empty null and duplicate outer keys
    When query
      """
      SELECT k, (SELECT count(*) FROM VALUES (1), (2) r(v) WHERE r.v < l.k) AS n
      FROM VALUES (1), (3), (3), (CAST(NULL AS INT)) l(k)
      ORDER BY k NULLS FIRST
      """
    Then query result collected ordered
      | k    | n |
      | NULL | 0 |
      | 1    | 0 |
      | 3    | 2 |
      | 3    | 2 |

  Scenario: An outer only predicate determines whether a scalar count sees any rows
    When query
      """
      SELECT k, (SELECT count(*) FROM VALUES (1), (2) r(v) WHERE l.k > 0) AS n
      FROM VALUES (0), (1), (CAST(NULL AS INT)) l(k)
      ORDER BY k NULLS FIRST
      """
    Then query result collected ordered
      | k    | n |
      | NULL | 0 |
      | 0    | 0 |
      | 1    | 2 |

  Scenario: An outer null key can match an outer only null predicate
    When query
      """
      SELECT k, (SELECT count(*) FROM VALUES (1), (2) r(v) WHERE l.k IS NULL) AS n
      FROM VALUES (0), (CAST(NULL AS INT)) l(k)
      ORDER BY k NULLS FIRST
      """
    Then query result collected ordered
      | k    | n |
      | NULL | 2 |
      | 0    | 0 |

  Scenario: An empty scalar aggregate default preserves null results from nonempty groups
    When query
      """
      SELECT k, (SELECT nullif(count(*), 2) FROM VALUES (1), (2) r(v) WHERE r.v < l.k) AS n
      FROM VALUES (1), (3) l(k)
      ORDER BY k
      """
    Then query result collected ordered
      | k | n    |
      | 1 | 0    |
      | 3 | NULL |

  Scenario: A lateral limit and offset apply separately for each outer key
    When query
      """
      SELECT l.k, q.v
      FROM VALUES (1), (2), (3) l(k) LEFT JOIN LATERAL (SELECT v FROM VALUES (1, 30), (1, 10), (1, 20), (2,
        40), (2, 50) r(k, v) WHERE r.k = l.k ORDER BY v LIMIT 2 OFFSET 1) q ON true
      ORDER BY l.k, q.v
      """
    Then query result collected ordered
      | k | v    |
      | 1 | 20   |
      | 1 | 30   |
      | 2 | 50   |
      | 3 | NULL |

  Scenario: A scalar descending limit chooses the largest value for each outer key
    When query
      """
      SELECT l.k, (SELECT v FROM VALUES (1, 30), (1, 10), (1, 20), (2, 40), (2, 50) r(k, v) WHERE r.k = l.k
        ORDER BY v DESC LIMIT 1) AS v
      FROM VALUES (1), (2), (3) l(k)
      ORDER BY l.k
      """
    Then query result collected ordered
      | k | v    |
      | 1 | 30   |
      | 2 | 50   |
      | 3 | NULL |

  Scenario: A correlated scalar subquery rejects multiple matching rows
    When query
      """
      SELECT (SELECT v FROM VALUES (1, 10), (1, 20) r(k, v) WHERE r.k = l.k)
      FROM VALUES (1) l(k)
      """
    Then query error (?i)(more than one row|too_many_rows)

  Scenario Outline: A scalar limit retains hidden ordering columns through projections
    When query
      """
      SELECT o.k,
        (SELECT v AS result
         FROM VALUES (1, 10, 3), (1, 20, 1), (1, 30, 1),
                     (2, 40, CAST(NULL AS INT)), (2, 50, 2) u(k, v, s)
         WHERE u.k = o.k
         ORDER BY <ordering>
         LIMIT 1) AS selected
      FROM VALUES (1), (2), (3) o(k)
      """
    Then query result collected
      | k | selected |
      | 1 | <first>  |
      | 2 | <second> |
      | 3 | NULL     |

    Examples:
      | ordering                            | first | second |
      | s + 1 ASC NULLS LAST, v DESC         | 30    | 50     |
      | s + 1 DESC NULLS FIRST, v ASC        | 10    | 40     |

  Scenario Outline: Projected membership evaluates an uncorrelated scalar aggregate
    When query
      """
      SELECT x,
        x IN (SELECT <aggregate> FROM VALUES (1), (2) u(v) WHERE <condition>) AS included,
        x NOT IN (SELECT <aggregate> FROM VALUES (1), (2) u(v) WHERE <condition>) AS excluded
      FROM VALUES (0L), (2L), (CAST(NULL AS BIGINT)) o(x)
      """
    Then query result collected
      | x    | included | excluded |
      | 0    | <zero>   | <notzero> |
      | 2    | <two>    | <nottwo> |
      | NULL | false    | false    |

    Examples:
      | aggregate | condition | zero  | notzero | two   | nottwo |
      | count(*)  | true      | false | true    | true  | false  |
      | count(*)  | false     | true  | false   | false | true   |
      | sum(v)    | false     | false | false   | false | false  |

  Scenario Outline: Correlated inequality aggregates preserve HAVING row existence
    When query
      """
      SELECT o.k,
        (SELECT <projection>
         FROM VALUES (1), (2) u(v)
         WHERE u.v < o.k
         HAVING <having>) AS result
      FROM VALUES (1), (2), (3), (3), (CAST(NULL AS INT)) o(k)
      """
    Then query result collected
      | k    | result  |
      | 1    | <empty> |
      | 2    | <one>   |
      | 3    | <two>   |
      | 3    | <two>   |
      | NULL | <empty> |

    Examples:
      | projection            | having             | empty | one  | two  |
      | count(*)              | count(*) > 0       | NULL  | 1    | 2    |
      | count(*)              | count(*) = 0       | 0     | NULL | NULL |
      | coalesce(sum(v), 99)   | count(*) > 0       | NULL  | 1    | 3    |
      | coalesce(sum(v), 99)   | sum(v) > 1         | NULL  | NULL | 3    |
      | coalesce(sum(v), 99)   | count(*) = 0       | 99    | NULL | NULL |

  Scenario: Scalar ordering recovery preserves distinct keys in nested projections
    When query
      """
      SELECT o.k,
        (SELECT DISTINCT v
         FROM (SELECT v
               FROM VALUES (1, 10, 3), (1, 10, 1) u(k, v, s)
               WHERE u.k = o.k
               ORDER BY s) d) AS result
      FROM VALUES (1), (2) o(k)
      """
    Then query result collected
      | k | result |
      | 1 | 10     |
      | 2 | NULL   |

  Scenario Outline: Scalar cardinality checks preserve a nested correlated ordered limit
    When query
      """
      SELECT k,
        (SELECT v FROM
          (SELECT v FROM VALUES (1, 10), (1, 20), (1, 30), (2, 40) u(k, v)
           WHERE u.k = t.k ORDER BY v DESC LIMIT 1 OFFSET <offset>) q) AS selected
      FROM VALUES (1), (2), (3) t(k)
      """
    Then query result collected
      | k | selected |
      | 1 | <first>  |
      | 2 | <second> |
      | 3 | NULL     |

    Examples:
      | offset | first | second |
      | 0      | 30    | 40     |
      | 1      | 20    | NULL   |

  Scenario Outline: Scalar cardinality checks preserve an uncorrelated inner ordered limit
    When query
      """
      SELECT k,
        (SELECT v FROM
          (SELECT k, v FROM VALUES (1, 10), (1, 20), (2, 30) u(k, v)
           ORDER BY v DESC LIMIT 1 OFFSET <offset>) q
         WHERE q.k = t.k) AS selected
      FROM VALUES (1), (2), (3) t(k)
      """
    Then query result collected
      | k | selected |
      | 1 | <first>  |
      | 2 | <second> |
      | 3 | NULL     |

    Examples:
      | offset | first | second |
      | 0      | NULL  | 30     |
      | 1      | 20    | NULL   |

  Scenario: Transferring a scalar ordering preserves a different inner top-k ordering
    When query
      """
      SELECT k,
        (SELECT v FROM
          (SELECT k, v FROM VALUES (1, 10), (1, 20), (1, 30) u(k, v)
           ORDER BY v DESC LIMIT 2) q
         WHERE q.k = t.k ORDER BY v ASC LIMIT 1) AS selected
      FROM VALUES (1), (2) t(k)
      """
    Then query result collected
      | k | selected |
      | 1 | 20       |
      | 2 | NULL     |

  Scenario Outline: Lateral limits retain hidden ordering through projections
    When query
      """
      SELECT o.k, q.v FROM VALUES (1), (2), (3) o(k),
      LATERAL (
        SELECT v FROM (
          SELECT <projection>
          FROM VALUES (1, 10, 3), (1, 20, 1), (1, 30, 2), (2, 40, 2), (2, 50, 1) u(k, v, s)
          WHERE u.k = o.k
        ) r ORDER BY <ordering> LIMIT 2 OFFSET 1
      ) q
      """
    Then query result collected
      | k | v  |
      | 1 | 30 |
      | 1 | 10 |
      | 2 | 40 |
    Then query column types
      | column | type |
      | k      | int  |
      | v      | int  |

    Examples:
      | projection       | ordering |
      | v, s, k          | r.s      |
      | v, s AS rank_key | rank_key |

  Scenario Outline: Scalar membership rejects multiple subquery output columns
    When query
      """
      SELECT 1 <operator> (SELECT 1, 2) AS result
      """
    Then query error (?i)(one column|column.*mismatch|number.*column)

    Examples:
      | operator |
      | IN       |
      | NOT IN   |

  Scenario Outline: Correlated aggregate membership retains its empty-input row
    When query
      """
      SELECT k, 0 <operator> (SELECT count(*) FROM VALUES (1) u(v) WHERE u.v = o.k) AS b
      FROM VALUES (1), (2), (CAST(NULL AS INT)) o(k)
      """
    Then query result collected
      | k    | b       |
      | 1    | <one>   |
      | 2    | <empty> |
      | NULL | <empty> |

    Examples:
      | operator | one   | empty |
      | IN       | false | true  |
      | NOT IN   | true  | false |

  Scenario: HAVING-only correlation does not require an aggregate domain
    When query
      """
      SELECT k, (SELECT count(*) FROM VALUES (1), (2) u(v) HAVING count(*) < o.k) AS n
      FROM VALUES (1), (3), (CAST(NULL AS INT)) o(k)
      """
    Then query result collected
      | k    | n    |
      | 1    | NULL |
      | 3    | 2    |
      | NULL | NULL |

  @sail-only
  Scenario: Correlated inequality aggregates reject independently evaluated volatile inputs
    When query
      """
      SELECT (SELECT count(*) FROM range(1) r WHERE r.id < o.k AND rand() < 0.5) AS n
      FROM VALUES (1) o(k)
      """
    Then query error (?i)correlated scalar aggregate with volatile inner expressions

  Scenario Outline: Scalar ordering carries hidden keys through a derived table alias
    When query
      """
      SELECT o.k,
        (SELECT v FROM (
           SELECT v FROM VALUES (1, 10, 3), (1, 20, 1),
                                (2, 30, CAST(NULL AS INT)), (2, 40, 2) u(k, v, s)
           WHERE u.k = o.k ORDER BY <ordering>
         ) q LIMIT 1) AS selected
      FROM VALUES (1), (2), (3) o(k)
      """
    Then query result collected
      | k | selected |
      | 1 | <one>    |
      | 2 | <two>    |
      | 3 | NULL     |

    Examples:
      | ordering           | one | two |
      | s ASC NULLS LAST   | 20  | 40  |
      | s DESC NULLS FIRST | 10  | 30  |

  Scenario: Scalar ordering carries hidden keys through multiple derived table aliases
    When query
      """
      SELECT (SELECT result FROM (
        SELECT v AS result FROM (
          SELECT v FROM VALUES (1, 10, 3), (1, 20, 1) u(k, v, s)
          WHERE u.k = o.k ORDER BY s
        ) q WHERE v > 0
      ) r LIMIT 1) AS selected
      FROM VALUES (1) o(k)
      """
    Then query result
      | selected |
      | 20       |

  Scenario Outline: Correlated aggregate membership distinguishes HAVING rejection from null values
    When query
      """
      SELECT o.k, o.x,
        x IN (SELECT <aggregate> FROM VALUES (1, CAST(NULL AS INT)), (3, 0) u(k, v)
              WHERE u.k = o.k HAVING count(*) > 0) AS included,
        x NOT IN (SELECT <aggregate> FROM VALUES (1, CAST(NULL AS INT)), (3, 0) u(k, v)
                  WHERE u.k = o.k HAVING count(*) > 0) AS excluded
      FROM VALUES (1, 0), (2, 0), (CAST(NULL AS INT), 0),
                  (1, CAST(NULL AS INT)), (2, CAST(NULL AS INT)), (3, 0), (3, 0) o(k, x)
      """
    Then query result collected
      | k    | x    | included | excluded   |
      | 1    | 0    | false    | <one>      |
      | 2    | 0    | false    | true       |
      | NULL | 0    | false    | true       |
      | 1    | NULL | false    | false      |
      | 2    | NULL | false    | true       |
      | 3    | 0    | <three>  | <notthree> |
      | 3    | 0    | <three>  | <notthree> |

    Examples:
      | aggregate | one   | three | notthree |
      | count(*)  | true  | false | true     |
      | sum(v)    | false | true  | false    |

  Scenario Outline: Correlated aggregate membership retains empty-input rows accepted by HAVING
    When query
      """
      SELECT o.k, o.x,
        x IN (SELECT <aggregate> FROM VALUES (1) u(v)
              WHERE u.v = o.k HAVING count(*) = 0) AS included,
        x NOT IN (SELECT <aggregate> FROM VALUES (1) u(v)
                  WHERE u.v = o.k HAVING count(*) = 0) AS excluded
      FROM VALUES (1, 0), (2, 0), (2, CAST(NULL AS INT)) o(k, x)
      """
    Then query result collected
      | k | x    | included | excluded |
      | 1 | 0    | false    | true     |
      | 2 | 0    | <empty>  | false    |
      | 2 | NULL | false    | false    |

    Examples:
      | aggregate | empty |
      | count(*)  | true  |
      | sum(v)    | false |

  Scenario: Correlated inequality membership preserves HAVING row existence
    When query
      """
      SELECT o.k,
        0 NOT IN (SELECT count(*) FROM VALUES (1), (2) u(v)
                  WHERE u.v < o.k HAVING count(*) > 0) AS excluded
      FROM VALUES (1), (3) o(k)
      """
    Then query result collected
      | k | excluded |
      | 1 | true     |
      | 3 | true     |

  Scenario Outline: Ordered correlated inequality aggregates preserve empty-input defaults
    When query
      """
      SELECT o.k,
        (SELECT <aggregate> AS result FROM VALUES (1), (2) u(v)
         WHERE u.v < o.k
         HAVING <having> ORDER BY result DESC) AS result
      FROM VALUES (1), (2), (3), (CAST(NULL AS INT)) o(k)
      """
    Then query result collected
      | k    | result  |
      | 1    | <empty> |
      | 2    | <one>   |
      | 3    | <two>   |
      | NULL | <empty> |

    Examples:
      | aggregate            | having        | empty | one | two |
      | count(*)             | count(*) >= 0 | 0     | 1   | 2   |
      | count(*)             | count(*) > 0  | NULL  | 1   | 2   |
      | sum(v)               | count(*) >= 0 | NULL  | 1   | 3   |
      | coalesce(sum(v), 99) | count(*) >= 0 | 99    | 1   | 3   |
