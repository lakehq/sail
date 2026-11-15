Feature: Join key and empty input resolution
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
      CREATE OR REPLACE TEMPORARY VIEW c AS SELECT *
      FROM VALUES (1L, 10, 100), (2L, 20, 200) AS c(id, A, a)
      """
    Given final statement
      """
      DROP VIEW IF EXISTS c
      """

  Scenario: Natural join matches differently cased key names
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT *
      FROM (SELECT 1 AS A) l NATURAL JOIN (SELECT 2 AS a) r
      """
    Then query result collected
      | A |
    Then query column types
      | column | type |
      | A      | int  |

  Scenario: Case insensitive USING retains a distinct column with a matching name
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT *
      FROM c JOIN (SELECT 10 AS A) r USING (A)
      """
    Then query result collected
      | A  | id | a   |
      | 10 | 1  | 100 |
    Then query column types
      | column | type   |
      | A      | int    |
      | id     | bigint |
      | a      | int    |

  Scenario: Case sensitive USING retains a differently cased nonkey column
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT *
      FROM c JOIN (SELECT 10 AS A) r USING (A)
      """
    Then query result collected
      | A  | id | a   |
      | 10 | 1  | 100 |
    Then query column types
      | column | type   |
      | A      | int    |
      | id     | bigint |
      | a      | int    |

  Scenario: Full USING join retains both qualified input keys
    When query
      """
      SELECT t.id AS l, u.id AS r, id AS k
      FROM t FULL JOIN u USING (id)
      """
    Then query result collected
      | l    | r    | k    |
      | 1    | 1    | 1    |
      | 2    | 2    | 2    |
      | 3    | NULL | 3    |
      | 4    | 4    | 4    |
      | 5    | NULL | 5    |
      | 6    | NULL | 6    |
      | 7    | NULL | 7    |
      | NULL | 8    | 8    |
      | NULL | NULL | NULL |
    Then query column types
      | column | type   |
      | l      | bigint |
      | r      | bigint |
      | k      | bigint |

  Scenario: Qualified stars retain each side of a full USING join
    When query
      """
      SELECT t.*, u.*
      FROM t FULL JOIN u USING (id)
      """
    Then query result collected
      | id   | g    | v    | w    | id   | g    | z    |
      | 1    | a    | 10   | 100  | 1    | a    | 100  |
      | 2    | a    | 20   | 200  | 2    | a    | 200  |
      | 3    | b    | NULL | 300  | NULL | NULL | NULL |
      | 4    | b    | 40   | NULL | 4    | b    | 400  |
      | 5    | NULL | 50   | 500  | NULL | NULL | NULL |
      | 6    | a    | 10   | 600  | NULL | NULL | NULL |
      | 7    | c    | 70   | 700  | NULL | NULL | NULL |
      | NULL | NULL | NULL | NULL | 8    | z    | 800  |
      | NULL | NULL | NULL | NULL | NULL | n    | 900  |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Natural join coalesces repeated references to the same key
    When query
      """
      SELECT *
      FROM (SELECT id, id FROM t) l NATURAL JOIN u
      """
    Then query result collected
      | id | g | z   |
      | 1  | a | 100 |
      | 2  | a | 200 |
      | 4  | b | 400 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Left join without a condition preserves unmatched projected keys
    When query
      """
      SELECT t.id AS l, u.id AS r
      FROM t LEFT JOIN (SELECT * FROM u WHERE false) u
      """
    Then query result collected
      | l | r    |
      | 1 | NULL |
      | 2 | NULL |
      | 3 | NULL |
      | 4 | NULL |
      | 5 | NULL |
      | 6 | NULL |
      | 7 | NULL |
    Then query column types
      | column | type   |
      | l      | bigint |
      | r      | bigint |

  Scenario: Natural join coalesces equal differently cased keys
    When query
      """
      SELECT *
      FROM (SELECT 1 AS A) l NATURAL JOIN (SELECT 1 AS a) r
      """
    Then query result collected
      | A |
      | 1 |
    Then query column types
      | column | type |
      | A      | int  |

  Scenario: Left join without a condition preserves rows when the right input is empty
    When query
      """
      SELECT *
      FROM t l LEFT JOIN (SELECT * FROM u WHERE false) r
      """
    Then query result collected
      | id | g    | v    | w    | id   | g    | z    |
      | 1  | a    | 10   | 100  | NULL | NULL | NULL |
      | 2  | a    | 20   | 200  | NULL | NULL | NULL |
      | 3  | b    | NULL | 300  | NULL | NULL | NULL |
      | 4  | b    | 40   | NULL | NULL | NULL | NULL |
      | 5  | NULL | 50   | 500  | NULL | NULL | NULL |
      | 6  | a    | 10   | 600  | NULL | NULL | NULL |
      | 7  | c    | 70   | 700  | NULL | NULL | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Right join without a condition preserves rows when the left input is empty
    When query
      """
      SELECT *
      FROM (SELECT * FROM t WHERE false) l RIGHT JOIN u r
      """
    Then query result collected
      | id   | g    | v    | w    | id   | g | z   |
      | NULL | NULL | NULL | NULL | 1    | a | 100 |
      | NULL | NULL | NULL | NULL | 2    | a | 200 |
      | NULL | NULL | NULL | NULL | 4    | b | 400 |
      | NULL | NULL | NULL | NULL | 8    | z | 800 |
      | NULL | NULL | NULL | NULL | NULL | n | 900 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Full join without a condition preserves the nonempty right input
    When query
      """
      SELECT *
      FROM (SELECT * FROM t WHERE false) l FULL JOIN u r
      """
    Then query result collected
      | id   | g    | v    | w    | id   | g | z   |
      | NULL | NULL | NULL | NULL | 1    | a | 100 |
      | NULL | NULL | NULL | NULL | 2    | a | 200 |
      | NULL | NULL | NULL | NULL | 4    | b | 400 |
      | NULL | NULL | NULL | NULL | 8    | z | 800 |
      | NULL | NULL | NULL | NULL | NULL | n | 900 |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Full join without a condition preserves the nonempty left input
    When query
      """
      SELECT *
      FROM t l FULL JOIN (SELECT * FROM u WHERE false) r
      """
    Then query result collected
      | id | g    | v    | w    | id   | g    | z    |
      | 1  | a    | 10   | 100  | NULL | NULL | NULL |
      | 2  | a    | 20   | 200  | NULL | NULL | NULL |
      | 3  | b    | NULL | 300  | NULL | NULL | NULL |
      | 4  | b    | 40   | NULL | NULL | NULL | NULL |
      | 5  | NULL | 50   | 500  | NULL | NULL | NULL |
      | 6  | a    | 10   | 600  | NULL | NULL | NULL |
      | 7  | c    | 70   | 700  | NULL | NULL | NULL |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Semi join without a condition returns no rows for an empty left input
    When query
      """
      SELECT *
      FROM (SELECT * FROM t WHERE false) l LEFT SEMI JOIN u r
      """
    Then query result collected
      | id | g | v | w |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |

  Scenario: Semi join without a condition returns no rows for an empty right input
    When query
      """
      SELECT *
      FROM t l LEFT SEMI JOIN (SELECT * FROM u WHERE false) r
      """
    Then query result collected
      | id | g | v | w |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |

  Scenario: Semi join without a condition retains each left row once
    When query
      """
      SELECT *
      FROM t l LEFT SEMI JOIN u r
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

  Scenario: Anti join without a condition returns no rows for an empty left input
    When query
      """
      SELECT *
      FROM (SELECT * FROM t WHERE false) l LEFT ANTI JOIN u r
      """
    Then query result collected
      | id | g | v | w |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |

  Scenario: Anti join without a condition retains all rows for an empty right input
    When query
      """
      SELECT *
      FROM t l LEFT ANTI JOIN (SELECT * FROM u WHERE false) r
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

  Scenario: Anti join without a condition returns no rows for a nonempty right input
    When query
      """
      SELECT *
      FROM t l LEFT ANTI JOIN u r
      """
    Then query result collected
      | id | g | v | w |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |

  Scenario: Grouping by a qualified USING key preserves unmatched null keys
    When query
      """
      SELECT u.id AS k, count(*) AS n
      FROM t LEFT JOIN u USING (id)
      GROUP BY u.id
      """
    Then query result collected
      | k    | n |
      | 1    | 1 |
      | 2    | 1 |
      | 4    | 1 |
      | NULL | 4 |
    Then query column types
      | column | type   |
      | k      | bigint |
      | n      | bigint |

  Scenario: Nested projections retain duplicate names from qualified USING stars
    When query
      """
      SELECT q.*
      FROM (SELECT t.*, u.* FROM t FULL JOIN u USING (id)) q
      """
    Then query result collected
      | id   | g    | v    | w    | id   | g    | z    |
      | 1    | a    | 10   | 100  | 1    | a    | 100  |
      | 2    | a    | 20   | 200  | 2    | a    | 200  |
      | 3    | b    | NULL | 300  | NULL | NULL | NULL |
      | 4    | b    | 40   | NULL | 4    | b    | 400  |
      | 5    | NULL | 50   | 500  | NULL | NULL | NULL |
      | 6    | a    | 10   | 600  | NULL | NULL | NULL |
      | 7    | c    | 70   | 700  | NULL | NULL | NULL |
      | NULL | NULL | NULL | NULL | 8    | z    | 800  |
      | NULL | NULL | NULL | NULL | NULL | n    | 900  |
    Then query column types
      | column | type   |
      | id     | bigint |
      | g      | string |
      | v      | int    |
      | w      | int    |
      | id     | bigint |
      | g      | string |
      | z      | int    |

  Scenario: Chained USING joins retain all qualified input keys
    When query
      """
      SELECT t.id AS l, u.id AS r, q.id AS q
      FROM t JOIN u USING (id) JOIN u q USING (id)
      """
    Then query result collected
      | l | r | q |
      | 1 | 1 | 1 |
      | 2 | 2 | 2 |
      | 4 | 4 | 4 |
    Then query column types
      | column | type   |
      | l      | bigint |
      | r      | bigint |
      | q      | bigint |
