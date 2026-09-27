Feature: Numeric STRING UNION casts stay lazy under window and grouping consumers

  Background:
    Given variable location for temporary directory union_conditional_source
    Given final statement
      """
      DROP TABLE IF EXISTS union_conditional_source
      """
    Given statement template
      """
      CREATE TABLE union_conditional_source (id BIGINT, kind STRING, v STRING)
      USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO union_conditional_source VALUES (1, 'n', '5'), (2, 's', 'bad'), (3, 'n', '7')
      """

  Scenario Outline: UNION conditional projection selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CASE WHEN kind = 'n' THEN v ELSE -1 END AS r
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | r  |
      | 1  | 5  |
      | 2  | -1 |
      | 3  | 7  |
      | 4  | 40 |
      | 5  | 50 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION conditional in a window select list selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CASE WHEN kind = 'n' THEN v ELSE -1 END AS r,
             typeof(CASE WHEN kind = 'n' THEN v ELSE -1 END) AS result_type,
             row_number() OVER (ORDER BY id) AS rn
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | r  | result_type | rn |
      | 1  | 5  | <type>      | 1  |
      | 2  | -1 | <type>      | 2  |
      | 3  | 7  | <type>      | 3  |
      | 4  | 40 | <type>      | 4  |
      | 5  | 50 | <type>      | 5  |

    Examples:
      | ansi  | type   |
      | true  | bigint |
      | false | string |

  Scenario Outline: UNION conditional next to a partitioned window selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CASE WHEN kind = 'n' THEN v ELSE -1 END AS r,
             count(*) OVER (PARTITION BY kind) AS c
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | r  | c |
      | 1  | 5  | 4 |
      | 2  | -1 | 1 |
      | 3  | 7  | 4 |
      | 4  | 40 | 4 |
      | 5  | 50 | 4 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION conditional as a window function argument selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CAST(sum(CASE WHEN kind = 'n' THEN v ELSE -1 END) OVER (ORDER BY id) AS BIGINT) AS s
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | s   |
      | 1  | 5   |
      | 2  | 4   |
      | 3  | 11  |
      | 4  | 51  |
      | 5  | 101 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION conditional as a window partition key selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, count(*) OVER (PARTITION BY CASE WHEN kind = 'n' THEN v ELSE 5 END) AS c
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | c |
      | 1  | 2 |
      | 2  | 2 |
      | 3  | 1 |
      | 4  | 1 |
      | 5  | 1 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION conditional as a window order key sorts <type> values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, row_number() OVER (ORDER BY CASE WHEN kind = 'n' THEN v ELSE 45 END, id) AS rn
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | rn    |
      | 1  | <rn1> |
      | 2  | <rn2> |
      | 3  | <rn3> |
      | 4  | <rn4> |
      | 5  | <rn5> |

    Examples:
      | ansi  | type    | rn1 | rn2 | rn3 | rn4 | rn5 |
      | true  | numeric | 1   | 4   | 2   | 3   | 5   |
      | false | STRING  | 3   | 2   | 5   | 1   | 4   |

  Scenario Outline: UNION conditional filtered by its window rank selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT * FROM (
        SELECT id, CASE WHEN kind = 'n' THEN v ELSE -1 END AS r,
               row_number() OVER (PARTITION BY kind ORDER BY id) AS rn
        FROM (
          SELECT id, kind, v FROM union_conditional_source
          UNION ALL
          SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
        )
      ) WHERE rn = 1 ORDER BY id
      """
    Then query result collected ordered
      | id | r  | rn |
      | 1  | 5  | 1  |
      | 2  | -1 | 1  |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION conditional grouping key <grouping> selects only valid STRING values with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT CASE WHEN kind = 'n' THEN v ELSE -1 END AS r, count(*) AS c
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) GROUP BY <grouping>
      """
    Then query result collected
      | r  | c |
      | -1 | 1 |
      | 5  | 1 |
      | 7  | 1 |
      | 40 | 1 |
      | 50 | 1 |

    Examples:
      | ansi  | grouping                                 |
      | true  | 1                                        |
      | true  | CASE WHEN kind = 'n' THEN v ELSE -1 END  |
      | false | 1                                        |
      | false | CASE WHEN kind = 'n' THEN v ELSE -1 END  |

  Scenario: ANSI UNION conditional aggregate argument retains the STRING cast error
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT kind, sum(CASE WHEN kind = 'n' THEN v ELSE -1 END) AS s
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) GROUP BY kind
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario: Non-ANSI UNION conditional aggregate argument sums STRING values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT kind, sum(CASE WHEN kind = 'n' THEN v ELSE -1 END) AS s
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) GROUP BY kind
      """
    Then query result collected
      | kind | s     |
      | n    | 102.0 |
      | s    | -1.0  |

  Scenario: ANSI UNION conditional above a join retains the STRING cast error
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT u.id, CASE WHEN u.kind = 'n' THEN u.v ELSE -1 END AS r
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) u JOIN range(10) s ON u.id = s.id
      """
    Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast)

  Scenario: Non-ANSI UNION conditional above a join keeps STRING values
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT u.id, CASE WHEN u.kind = 'n' THEN u.v ELSE -1 END AS r
      FROM (
        SELECT id, kind, v FROM union_conditional_source
        UNION ALL
        SELECT id, 'n' AS kind, id * 10 AS v FROM range(4, 6)
      ) u JOIN range(10) s ON u.id = s.id
      ORDER BY u.id
      """
    Then query result collected ordered
      | id | r  |
      | 1  | 5  |
      | 2  | -1 |
      | 3  | 7  |
      | 4  | 40 |
      | 5  | 50 |
