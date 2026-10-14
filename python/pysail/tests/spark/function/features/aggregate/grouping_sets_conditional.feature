Feature: Grouping analytics over conditional and IN-list grouping expressions

  Rule: Grouping sets keep their grouping ID when a member is an IN list or NVL2

    Scenario: ROLLUP over an IN list with column values
      When query
        """
        SELECT a IN (b, c, 1, 2) AS k, grouping(a IN (b, c, 1, 2)) AS g, count(*) AS n
        FROM VALUES
          (0, 0, 0, CAST(NULL AS INT)), (1, 1, 1, 1), (2, 2, 0, 2),
          (3, 1, 3, CAST(NULL AS INT)), (4, 4, 4, 0), (5, 3, 2, 5) AS t(id, a, b, c)
        GROUP BY ROLLUP(a IN (b, c, 1, 2))
        ORDER BY g, k NULLS FIRST
        """
      Then query result ordered
        | k     | g | n |
        | false | 0 | 1 |
        | true  | 0 | 5 |
        | NULL  | 1 | 6 |

    Scenario: CUBE over an IN list and a column
      When query
        """
        SELECT a IN (b, c, 1, 2) AS k, a, grouping_id() AS gid, count(*) AS n
        FROM VALUES
          (0, 0, 0, CAST(NULL AS INT)), (1, 1, 1, 1), (2, 2, 0, 2),
          (3, 1, 3, CAST(NULL AS INT)), (4, 4, 4, 0), (5, 3, 2, 5) AS t(id, a, b, c)
        GROUP BY CUBE(a IN (b, c, 1, 2), a)
        ORDER BY gid, k NULLS FIRST, a NULLS FIRST
        """
      Then query result ordered
        | k     | a    | gid | n |
        | false | 3    | 0   | 1 |
        | true  | 0    | 0   | 1 |
        | true  | 1    | 0   | 2 |
        | true  | 2    | 0   | 1 |
        | true  | 4    | 0   | 1 |
        | false | NULL | 1   | 1 |
        | true  | NULL | 1   | 5 |
        | NULL  | 0    | 2   | 1 |
        | NULL  | 1    | 2   | 2 |
        | NULL  | 2    | 2   | 1 |
        | NULL  | 3    | 2   | 1 |
        | NULL  | 4    | 2   | 1 |
        | NULL  | NULL | 3   | 6 |

    Scenario: GROUPING SETS over NVL2 with column branches
      When query
        """
        SELECT nvl2(c, a, b) AS k, grouping(nvl2(c, a, b)) AS g, sum(id) AS s
        FROM VALUES
          (0, 0, 0, CAST(NULL AS INT)), (1, 1, 1, 1), (2, 2, 0, 2),
          (3, 1, 3, CAST(NULL AS INT)), (4, 4, 4, 0), (5, 3, 2, 5) AS t(id, a, b, c)
        GROUP BY GROUPING SETS ((nvl2(c, a, b)), ())
        ORDER BY g, k NULLS FIRST
        """
      Then query result ordered
        | k    | g | s  |
        | 0    | 0 | 0  |
        | 1    | 0 | 1  |
        | 2    | 0 | 2  |
        | 3    | 0 | 8  |
        | 4    | 0 | 4  |
        | NULL | 1 | 15 |

    Scenario: ROLLUP over NVL2 with literal branches
      When query
        """
        SELECT nvl2(c, 1, 2) AS k, count(*) AS n
        FROM VALUES
          (0, 0, 0, CAST(NULL AS INT)), (1, 1, 1, 1), (2, 2, 0, 2),
          (3, 1, 3, CAST(NULL AS INT)), (4, 4, 4, 0), (5, 3, 2, 5) AS t(id, a, b, c)
        GROUP BY ROLLUP(nvl2(c, 1, 2))
        ORDER BY k NULLS FIRST, n
        """
      Then query result ordered
        | k    | n |
        | NULL | 6 |
        | 1    | 4 |
        | 2    | 2 |
