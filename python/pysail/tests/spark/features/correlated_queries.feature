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
