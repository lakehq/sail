Feature: Scalar subquery nullability

  @function(nullability)
  Rule: Uncorrelated scalar subqueries are nullable

    Scenario: an empty scalar subquery evaluates to NULL with a nullable schema
      When query
        """
        SELECT (SELECT 1 WHERE false) AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    Scenario: a scalar subquery with exactly one row is still nullable
      When query
        """
        SELECT (SELECT 1) AS c
        """
      Then query result collected
        | c |
        | 1 |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    Scenario: a scalar count subquery over empty input is nullable
      When query
        """
        SELECT (SELECT count(*) FROM range(0)) AS c
        """
      Then query result collected
        | c |
        | 0 |
      Then query schema
        """
        root
         |-- c: long (nullable = true)
        """

    Scenario: a scalar max subquery is nullable
      When query
        """
        SELECT (SELECT max(id) FROM range(3)) AS c
        """
      Then query result collected
        | c |
        | 2 |
      Then query schema
        """
        root
         |-- c: long (nullable = true)
        """

    Scenario: adding to an empty scalar subquery yields a nullable NULL
      When query
        """
        SELECT (SELECT 1 WHERE false) + 1 AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    Scenario: modulo one on an empty scalar subquery is not folded to zero
      When query
        """
        SELECT (SELECT 5 WHERE false) % 1 AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    Scenario: comparing two empty scalar subqueries yields a nullable NULL
      When query
        """
        SELECT (SELECT 1 WHERE false) = (SELECT 1 WHERE false) AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: boolean (nullable = true)
        """

    Scenario: casting an empty scalar subquery keeps it nullable
      When query
        """
        SELECT CAST((SELECT 1 WHERE false) AS STRING) AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: string (nullable = true)
        """

    Scenario: a struct field built from an empty scalar subquery is nullable
      When query
        """
        SELECT struct((SELECT 1 WHERE false) AS x) AS s
        """
      Then query result
        | s      |
        | {NULL} |
      Then query schema
        """
        root
         |-- s: struct (nullable = false)
         |    |-- x: integer (nullable = true)
        """

    Scenario: an array element built from an empty scalar subquery is nullable
      When query
        """
        SELECT array((SELECT 1 WHERE false)) AS a
        """
      Then query result
        | a      |
        | [NULL] |
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

  Rule: Null checks on scalar subqueries are evaluated rather than folded

    Scenario Outline: <expression> evaluates to <expected>
      When query
        """
        SELECT <expression> AS c
        """
      Then query result collected
        | c          |
        | <expected> |

      Examples:
        | expression                              | expected |
        | (SELECT 1 WHERE false) IS NULL          | true     |
        | (SELECT 1 WHERE false) IS NOT NULL      | false    |
        | isnull((SELECT 1 WHERE false))          | true     |
        | isnotnull((SELECT 1 WHERE false))       | false    |
        | (SELECT 1 WHERE false) <=> NULL         | true     |
        | (SELECT id FROM range(1)) IS NULL       | false    |
        | (SELECT count(*) FROM range(0)) IS NULL | false    |

  Rule: Null-handling functions receive the NULL of an empty scalar subquery

    Scenario Outline: <function> falls back to its alternative value
      When query
        """
        SELECT <expression> AS c
        """
      Then query result collected
        | c          |
        | <expected> |

      Examples:
        | function | expression                             | expected |
        | coalesce | coalesce((SELECT 1 WHERE false), 2)    | 2        |
        | nvl      | nvl((SELECT 1 WHERE false), 2)         | 2        |
        | ifnull   | ifnull((SELECT 1 WHERE false), 2)      | 2        |
        | nvl2     | nvl2((SELECT 1 WHERE false), 'a', 'b') | b        |

  Rule: Empty scalar subqueries flow through filters and conditionals

    Scenario: a filter on an empty scalar subquery being NULL keeps every row
      When query
        """
        SELECT id FROM range(2) WHERE (SELECT 1 WHERE false) IS NULL ORDER BY id
        """
      Then query result collected ordered
        | id |
        | 0  |
        | 1  |

    Scenario: a filter on an empty scalar subquery being not NULL drops every row
      When query
        """
        SELECT count(*) AS c FROM range(3) WHERE (SELECT 1 WHERE false) IS NOT NULL
        """
      Then query result collected
        | c |
        | 0 |

    Scenario: a CASE branch on an empty scalar subquery being NULL is taken
      When query
        """
        SELECT CASE WHEN (SELECT 1 WHERE false) IS NULL THEN 'empty' ELSE 'row' END AS c
        """
      Then query result collected
        | c     |
        | empty |

    Scenario Outline: an IN list over scalar subqueries evaluates <expression> to <expected>
      When query
        """
        SELECT <expression> AS c
        """
      Then query result collected
        | c          |
        | <expected> |

      Examples:
        | expression                       | expected |
        | (SELECT 1 WHERE false) IN (1, 2) | NULL     |
        | 1 IN ((SELECT 1 WHERE false))    | NULL     |
        | 1 IN ((SELECT 1))                | true     |
        | 2 IN ((SELECT 1), 2)             | true     |
        | 2 IN ((SELECT 1 WHERE false), 2) | true     |
        | 1 IN ((SELECT 1 WHERE false), 2) | NULL     |

    Scenario: an IN list over a multi-row scalar subquery errors
      When query
        """
        SELECT 1 IN ((SELECT id FROM range(2))) AS c
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

  Rule: Null checks on scalar subqueries stay nullable inside conditionals and aggregates

    @function(nullability)
    Scenario: a CASE branch selected by an empty scalar subquery yields a nullable NULL
      When query
        """
        SELECT CASE WHEN (SELECT 1 WHERE false) IS NULL THEN NULL ELSE 1 END AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    @function(nullability)
    Scenario: a CASE branch selected by an empty aggregate subquery yields a nullable NULL
      When query
        """
        SELECT CASE WHEN (SELECT max(id) FROM range(0)) IS NULL THEN NULL ELSE 1 END AS c
        """
      Then query result collected
        | c    |
        | NULL |
      Then query schema
        """
        root
         |-- c: integer (nullable = true)
        """

    Scenario: count skips CASE results that an empty scalar subquery turns into NULL
      When query
        """
        SELECT count(CASE WHEN (SELECT 1 WHERE false) IS NULL THEN NULL ELSE 1 END) AS c FROM range(3)
        """
      Then query result collected
        | c |
        | 0 |

    Scenario: sum over CASE results that an empty scalar subquery turns into NULL is NULL
      When query
        """
        SELECT sum(x) AS c
        FROM (SELECT CASE WHEN (SELECT 1 WHERE false) IS NULL THEN NULL ELSE 1 END AS x FROM range(3))
        """
      Then query result collected
        | c    |
        | NULL |

    Scenario: count over a derived column nulled by an empty scalar subquery is zero
      When query
        """
        SELECT count(x) AS c
        FROM (SELECT CASE WHEN (SELECT 1 WHERE false) IS NULL THEN NULL ELSE id END AS x FROM range(3))
        """
      Then query result collected
        | c |
        | 0 |

    Scenario: count over a derived column kept by a non-empty scalar subquery counts every row
      When query
        """
        SELECT count(x) AS c
        FROM (SELECT CASE WHEN (SELECT id FROM range(1)) IS NULL THEN NULL ELSE id END AS x FROM range(3))
        """
      Then query result collected
        | c |
        | 3 |

  Rule: Empty scalar subqueries in grouping, window and join expressions

    @function(nullability)
    Scenario: grouping by an empty scalar subquery yields one nullable NULL group
      When query
        """
        SELECT (SELECT 1 WHERE false) AS k, count(*) AS c FROM range(2) GROUP BY (SELECT 1 WHERE false)
        """
      Then query result collected
        | k    | c |
        | NULL | 2 |
      Then query schema
        """
        root
         |-- k: integer (nullable = true)
         |-- c: long (nullable = false)
        """

    Scenario: partitioning a window by an empty scalar subquery keeps every row in one partition
      When query
        """
        SELECT id, count(*) OVER (PARTITION BY (SELECT 1 WHERE false)) AS c FROM range(2) ORDER BY id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 2 |
        | 1  | 2 |

    Scenario: a join condition on an empty scalar subquery being NULL matches every pair
      When query
        """
        SELECT t.a, u.b FROM VALUES (1) AS t(a) JOIN VALUES (2) AS u(b) ON (SELECT 1 WHERE false) IS NULL
        """
      Then query result collected
        | a | b |
        | 1 | 2 |

    Scenario: a join condition comparing with an empty scalar subquery matches nothing
      When query
        """
        SELECT t.a, u.b FROM VALUES (1) AS t(a) JOIN VALUES (2) AS u(b) ON t.a = (SELECT 1 WHERE false)
        """
      Then query result collected
        | a | b |

    Scenario: a left join condition comparing with an empty scalar subquery keeps unmatched left rows
      When query
        """
        SELECT t.a, u.b FROM VALUES (1) AS t(a) LEFT JOIN VALUES (2) AS u(b) ON t.a = (SELECT 1 WHERE false)
        """
      Then query result collected
        | a | b    |
        | 1 | NULL |

  @function(nullability)
  Rule: Correlated scalar subqueries are nullable

    Scenario: a correlated scalar subquery without a match yields a nullable NULL
      When query
        """
        SELECT a, (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a) AS c
        FROM VALUES (1), (2) AS t(a)
        ORDER BY a
        """
      Then query result collected ordered
        | a | c    |
        | 1 | 1    |
        | 2 | NULL |
      Then query schema
        """
        root
         |-- a: integer (nullable = false)
         |-- c: integer (nullable = true)
        """

    Scenario: a correlated scalar count is nullable
      When query
        """
        SELECT a, (SELECT count(*) FROM VALUES (1) AS u(b) WHERE u.b = t.a) AS c
        FROM VALUES (1), (2) AS t(a)
        ORDER BY a
        """
      Then query result collected ordered
        | a | c |
        | 1 | 1 |
        | 2 | 0 |
      Then query schema
        """
        root
         |-- a: integer (nullable = false)
         |-- c: long (nullable = true)
        """

    Scenario: a correlated scalar sum without a match yields a nullable NULL
      When query
        """
        SELECT a, (SELECT sum(b) FROM VALUES (1) AS u(b) WHERE u.b = t.a) AS c
        FROM VALUES (1), (2) AS t(a)
        ORDER BY a
        """
      Then query result collected ordered
        | a | c    |
        | 1 | 1    |
        | 2 | NULL |
      Then query schema
        """
        root
         |-- a: integer (nullable = false)
         |-- c: long (nullable = true)
        """

  Rule: Null checks on correlated scalar subqueries are evaluated per row

    Scenario: a filter on a correlated scalar subquery being NULL keeps unmatched rows
      When query
        """
        SELECT a FROM VALUES (1), (2) AS t(a)
        WHERE (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a) IS NULL
        """
      Then query result collected
        | a |
        | 2 |

    Scenario: a projected null check on a correlated scalar subquery reflects the match
      When query
        """
        SELECT a, (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a) IS NOT NULL AS m
        FROM VALUES (1), (2) AS t(a)
        ORDER BY a
        """
      Then query result collected ordered
        | a | m     |
        | 1 | true  |
        | 2 | false |

    Scenario: a correlated scalar count is never NULL
      When query
        """
        SELECT a FROM VALUES (1), (2) AS t(a)
        WHERE (SELECT count(*) FROM VALUES (1) AS u(b) WHERE u.b = t.a) IS NULL
        """
      Then query result collected
        | a |

    Scenario: coalesce on a correlated scalar subquery falls back for unmatched rows
      When query
        """
        SELECT a, coalesce((SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a), -1) AS c
        FROM VALUES (1), (2) AS t(a)
        ORDER BY a
        """
      Then query result collected ordered
        | a | c  |
        | 1 | 1  |
        | 2 | -1 |

    Scenario: comparing a correlated scalar subquery with itself keeps only matched rows
      When query
        """
        SELECT a FROM VALUES (1), (2) AS t(a)
        WHERE (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a)
          = (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a)
        """
      Then query result collected
        | a |
        | 1 |
