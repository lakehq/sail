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

  Scenario Outline: GROUP BY ALL expands wildcard grouping keys
    When query
      """
      SELECT <projection>, count(*) AS n
      FROM VALUES (1, 10), (1, 10), (2, 20) source(v, w)
      GROUP BY ALL
      """
    Then query result collected
      | v | w  | n |
      | 1 | 10 | 2 |
      | 2 | 20 | 1 |

    Examples:
      | projection |
      | *          |
      | source.*   |

  Scenario: GROUP BY ALL excludes columns removed by star EXCEPT
    When query
      """
      SELECT * EXCEPT (w), count(*) AS n
      FROM VALUES (1, 10), (1, 20), (2, 30) source(v, w)
      GROUP BY ALL
      """
    Then query result collected
      | v | n |
      | 1 | 2 |
      | 2 | 1 |

  Scenario Outline: GROUP BY ALL matches explicit ordinal grouping
    When query
      """
      SELECT k, count(*) AS n FROM VALUES (1), (1), (2) AS t(k)
      GROUP BY <grouping> ORDER BY k
      """
    Then query result ordered
      | k | n |
      | 1 | 2 |
      | 2 | 1 |
    Then query column types
      | column | type   |
      | k      | int    |
      | n      | bigint |

    Examples:
      | grouping |
      | ALL      |
      | 1        |

  Scenario: GROUP BY ALL respects a case-sensitive input column
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT count(*) AS n FROM VALUES (1), (2) t(ALL) GROUP BY ALL
      """
    Then query result collected
      | n |
      | 1 |
      | 1 |

  Scenario: ORDER BY ALL respects a case-sensitive input column
    Given config spark.sql.caseSensitive = true
    When query
      """
      SELECT * FROM VALUES (1, 2), (2, 1) t(x, ALL) ORDER BY ALL
      """
    Then query result collected ordered
      | x | ALL |
      | 2 | 1   |
      | 1 | 2   |

  Scenario: GROUP BY ALL keeps inferred integer literals as expressions
    When query
      """
      SELECT 42, count(*) AS n FROM VALUES (1), (2) t(v) GROUP BY ALL
      """
    Then query result collected
      | 42 | n |
      | 42 | 2 |

  Scenario Outline: A matching select alias takes precedence over GROUP BY ALL inference
    Given config spark.sql.caseSensitive = <sensitive>
    When query
      """
      SELECT k AS <alias>, v, count(*) AS n
      FROM VALUES (1, 10), (1, 20) t(k, v)
      GROUP BY ALL
      """
    Then query error (?i)(non-aggregate|not.*group by|missing_aggregation|no field named)

    Examples:
      | sensitive | alias |
      | false     | ALL   |
      | false     | all   |
      | true      | ALL   |

  Scenario Outline: GROUP BY ALL infers keys when select alias resolution is inapplicable
    Given config spark.sql.caseSensitive = <sensitive>
    Given config spark.sql.groupByAliases = <aliases>
    When query
      """
      SELECT k AS <alias>, v, count(*) AS n
      FROM VALUES (1, 10), (1, 20) t(k, v)
      GROUP BY ALL
      """
    Then query result collected
      | <alias> | v  | n |
      | 1       | 10 | 1 |
      | 1       | 20 | 1 |

    Examples:
      | sensitive | aliases | alias |
      | false     | false   | ALL   |
      | true      | false   | ALL   |
      | true      | true    | all   |

  Scenario: ORDER BY ALL excludes hidden USING keys
    When query
      """
      SELECT * FROM VALUES (1, CAST(NULL AS INT), CAST(NULL AS STRING)) l(k1, k2, a)
      FULL JOIN VALUES (1, CAST(NULL AS INT), 'a') r(k1, k2, b) USING (k1, k2)
      ORDER BY ALL
      """
    Then query result collected ordered
      | k1 | k2   | a    | b    |
      | 1  | NULL | NULL | NULL |
      | 1  | NULL | NULL | a    |

  Scenario Outline: Aggregate ordering uses the aggregate output after grouping-set expansion
    When query
      """
      SELECT a, b, count(*) AS n
      FROM VALUES (1, 10), (1, 20), (2, 20) t(a, b)
      GROUP BY <grouping>
      HAVING count(*) > 0
      ORDER BY count(*) DESC, a, b
      LIMIT 1
      """
    Then query result collected
      | a    | b    | n |
      | NULL | NULL | 3 |

    Examples:
      | grouping                       |
      | ROLLUP(a, b)                   |
      | CUBE(a, b)                     |
      | GROUPING SETS ((a, b), (a), ()) |

  Scenario Outline: ORDER BY ALL recovers a real unselected input column
    Given config spark.sql.caseSensitive = <sensitive>
    When query
      """
      SELECT x FROM VALUES (1, 2), (2, 1) t(x, ALL) ORDER BY ALL
      """
    Then query result collected ordered
      | x |
      | 2 |
      | 1 |

    Examples:
      | sensitive |
      | true      |
      | false     |

  Scenario Outline: Aggregate sort arguments resolve grouping keys through output aliases
    When query
      """
      SELECT k, sum(k) AS s
      FROM VALUES (1), (1), (3), (CAST(NULL AS INT)) source(k)
      GROUP BY k
      <having>
      ORDER BY <ordering> NULLS FIRST, k
      """
    Then query result ordered
      | k    | s    |
      | NULL | NULL |
      | 1    | 2    |
      | 3    | 3    |

    Examples:
      | having                              | ordering   |
      |                                     | sum(k)     |
      |                                     | sum(k) + 1 |
      | HAVING sum(k) IS NULL OR sum(k) > 0 | sum(k)     |

  Scenario: A hidden sort aggregate resolves its grouping key argument
    When query
      """
      SELECT k
      FROM VALUES (1), (1), (3) source(k)
      GROUP BY k
      ORDER BY sum(k) DESC
      """
    Then query result ordered
      | k |
      | 3 |
      | 1 |

  Scenario: An aggregate sort resolves a computed grouping expression
    When query
      """
      SELECT k + 1 AS g, sum(k + 1) AS s
      FROM VALUES (1), (1), (4) source(k)
      GROUP BY k + 1
      ORDER BY sum(k + 1) DESC
      """
    Then query result ordered
      | g | s |
      | 5 | 5 |
      | 2 | 4 |

  Scenario Outline: Grouping and ordering options accept mixed case booleans
    Given config spark.sql.groupByAliases = <value>
    Given config spark.sql.groupByOrdinal = <value>
    Given config spark.sql.orderByOrdinal = <value>
    When query
      """
      SELECT 1 AS x GROUP BY 1 ORDER BY 1
      """
    Then query result collected
      | x |
      | 1 |

    Examples:
      | value |
      | FALSE |
      | FaLsE |
      | TRUE  |
      | TrUe  |
