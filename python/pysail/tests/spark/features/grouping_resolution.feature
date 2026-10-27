Feature: Grouping and sort resolution
  Background:
    Given config spark.sql.caseSensitive = false
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.groupByAliases = true
    Given config spark.sql.groupByOrdinal = true
    Given config spark.sql.orderByOrdinal = true
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

  Scenario: ORDER BY ALL sorts by every output column
    When query
      """
      SELECT g, v, id
      FROM t
      ORDER BY ALL
      """
    Then query result collected ordered
      | g    | v    | id |
      | NULL | 50   | 5  |
      | a    | 10   | 1  |
      | a    | 10   | 6  |
      | a    | 20   | 2  |
      | b    | NULL | 3  |
      | b    | 40   | 4  |
      | c    | 70   | 7  |
    Then query column types
      | column | type   |
      | g      | string |
      | v      | int    |
      | id     | bigint |

  Scenario: Case sensitive grouping rejects a differently cased select alias
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT g AS Foo, count(*) AS n
      FROM t
      GROUP BY foo
      """
    Then query error (?i)(cannot resolve|unresolved|not found)

  Scenario: Case sensitive HAVING rejects a differently cased aggregate alias
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT g, count(*) AS N
      FROM t
      GROUP BY g
      HAVING n > 1
      """
    Then query error (?i)(cannot resolve|unresolved|not found)

  Scenario: Case sensitive USING rejects a differently cased right key
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT *
      FROM (SELECT 1 AS A) l JOIN (SELECT 1 AS a) r USING (A)
      """
    Then query error (?i)(cannot resolve|unresolved|not found)

  Scenario: GROUP BY ALL infers nonaggregate grouping expressions
    When query
      """
      SELECT g, sum(v) AS s
      FROM t
      GROUP BY ALL
      """
    Then query result collected
      | g    | s  |
      | a    | 40 |
      | b    | 40 |
      | c    | 70 |
      | NULL | 50 |
    Then query column types
      | column | type   |
      | g      | string |
      | s      | bigint |

  Scenario: ORDER BY resolves an aggregate absent from the select list
    When query
      """
      SELECT g
      FROM t
      GROUP BY g
      ORDER BY sum(v) NULLS FIRST, g NULLS FIRST
      """
    Then query result collected ordered
      | g    |
      | a    |
      | b    |
      | NULL |
      | c    |
    Then query column types
      | column | type   |
      | g      | string |

  Scenario: HAVING can filter rollup rows using grouping_id without arguments
    When query
      """
      SELECT g, grouping_id() AS gid, sum(v) AS s
      FROM t
      GROUP BY ROLLUP(g)
      HAVING grouping_id() = 1
      """
    Then query result collected
      | g    | gid | s   |
      | NULL | 1   | 200 |
    Then query column types
      | column | type   |
      | g      | string |
      | gid    | bigint |
      | s      | bigint |

  Scenario: A hidden aggregate sort uses default null ordering
    When query
      """
      SELECT g
      FROM t
      GROUP BY g
      ORDER BY sum(v), g NULLS FIRST
      """
    Then query result collected ordered
      | g    |
      | a    |
      | b    |
      | NULL |
      | c    |
    Then query column types
      | column | type   |
      | g      | string |

  Scenario: HAVING can filter rollup rows using an explicit grouping_id argument
    When query
      """
      SELECT g, grouping_id(g) AS gid, sum(v) AS s
      FROM t
      GROUP BY ROLLUP(g)
      HAVING grouping_id(g) = 1
      """
    Then query result collected
      | g    | gid | s   |
      | NULL | 1   | 200 |
    Then query column types
      | column | type   |
      | g      | string |
      | gid    | bigint |
      | s      | bigint |

  Scenario: Disabling grouping aliases rejects select aliases in GROUP BY
    Given config spark.sql.groupByAliases = false
    When query
      """
      SELECT v + 1 AS x, count(*) AS n
      FROM t
      GROUP BY x
      """
    Then query error (?i)(cannot resolve|unresolved|not found)

  Scenario: Disabling grouping ordinals treats GROUP BY numbers as literals
    Given config spark.sql.groupByOrdinal = false
    When query
      """
      SELECT g, count(*) AS n
      FROM t
      GROUP BY 1
      """
    Then query error (?i)(missing_aggregation|no field named)

  Scenario: Disabling sort ordinals treats ORDER BY numbers as literals
    Given config spark.sql.orderByOrdinal = false
    When query
      """
      SELECT id, v
      FROM t
      ORDER BY 2 NULLS LAST, id
      """
    Then query result collected ordered
      | id | v    |
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
      | v      | int    |

  Scenario: Sort ordinals and hidden aggregates resolve together
    When query
      """
      SELECT g
      FROM t
      GROUP BY g
      ORDER BY 1, sum(v)
      """
    Then query result collected ordered
      | g    |
      | NULL |
      | a    |
      | b    |
      | c    |
    Then query column types
      | column | type   |
      | g      | string |

  Scenario: HAVING can filter rollup rows using grouping
    When query
      """
      SELECT g, grouping(g) AS b, sum(v) AS s
      FROM t
      GROUP BY ROLLUP(g)
      HAVING grouping(g) = 1
      """
    Then query result collected
      | g    | b | s   |
      | NULL | 1 | 200 |
    Then query column types
      | column | type    |
      | g      | string  |
      | b      | tinyint |
      | s      | bigint  |

  Scenario: HAVING does not expose a missing aggregate to ORDER BY
    When query
      """
      SELECT g
      FROM t
      GROUP BY g
      HAVING count(*) > 0
      ORDER BY sum(v), g NULLS FIRST
      """
    Then query error (?i)(cannot resolve|unresolved|not found)

  Scenario: HAVING with a hidden count does not expose a missing sort aggregate
    When query
      """
      SELECT g
      FROM t
      GROUP BY g
      HAVING count(*) > 1
      ORDER BY max(w) DESC, g
      """
    Then query error (?i)(cannot resolve|unresolved|not found)
