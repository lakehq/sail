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

    Scenario Outline: <expression> over an invalid cast subquery raises the cast error
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

      Examples:
        | expression                              |
        | (SELECT CAST('bad' AS INT)) IS NULL     |
        | (SELECT CAST('bad' AS INT)) IS NOT NULL |
        | isnull((SELECT CAST('bad' AS INT)))     |

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

    Scenario: two correlated scalar subqueries in one filter are both evaluated
      When query
        """
        SELECT a FROM VALUES (1), (2), (3) AS t(a)
        WHERE (SELECT max(b) FROM VALUES (1), (2) AS u(b) WHERE u.b = t.a) = a
          AND (SELECT min(c) FROM VALUES (1), (3) AS v(c) WHERE v.c = t.a) = a
        """
      Then query result collected
        | a |
        | 1 |

    @function(nullability)
    Scenario: count over a correlated scalar subquery column skips unmatched rows
      When query
        """
        SELECT a, count(c) AS n
        FROM (SELECT a, (SELECT b FROM VALUES (1) AS u(b) WHERE u.b = t.a) AS c FROM VALUES (1), (2) AS t(a))
        GROUP BY a
        ORDER BY a
        """
      Then query result collected ordered
        | a | n |
        | 1 | 1 |
        | 2 | 0 |
      Then query schema
        """
        root
         |-- a: integer (nullable = false)
         |-- n: long (nullable = false)
        """

    Scenario: a filter on a non-equality correlated count being NULL keeps no rows
      When query
        """
        SELECT o.id FROM range(2) o WHERE (SELECT count(*) FROM VALUES (5), (7) AS t(x) WHERE x < o.id) IS NULL
        """
      Then query result collected
        | id |

    Scenario: a filter on a non-equality correlated count being not NULL keeps every row
      When query
        """
        SELECT o.id FROM range(2) o
        WHERE (SELECT count(*) FROM VALUES (5), (7) AS t(x) WHERE x < o.id) IS NOT NULL
        ORDER BY o.id
        """
      Then query result collected ordered
        | id |
        | 0  |
        | 1  |

  Rule: Correlated scalar subqueries inside aggregate arguments

    Scenario: count of a correlated count subquery counts every outer row
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE i.id = o.id)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of a correlated max subquery skips outer rows without a match
      When query
        """
        SELECT count((SELECT max(i.id) FROM range(1) i WHERE i.id = o.id)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 1 |

    Scenario: sum of a correlated max subquery adds the matched values
      When query
        """
        SELECT sum((SELECT max(i.id) FROM range(3) i WHERE i.id = o.id)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 1 |

    Scenario: grouped count of a correlated max subquery counts matches per group
      When query
        """
        SELECT o.id % 2 AS k, count((SELECT max(i.id) FROM range(1) i WHERE i.id = o.id)) AS n
        FROM range(4) o
        GROUP BY o.id % 2
        ORDER BY k
        """
      Then query result collected ordered
        | k | n |
        | 0 | 1 |
        | 1 | 0 |

    Scenario: HAVING over a count of a correlated max subquery keeps groups with matches
      When query
        """
        SELECT o.id % 2 AS k
        FROM range(4) o
        GROUP BY o.id % 2
        HAVING count((SELECT max(i.id) FROM range(1) i WHERE i.id = o.id)) > 0
        """
      Then query result collected
        | k |
        | 0 |

    Scenario: count of a correlated count subquery filtered only by the outer row counts every outer row
      When query
        """
        SELECT count((SELECT count(*) FROM range(1) i WHERE o.id >= 0)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of a correlated count subquery with a non-equality correlation counts every outer row
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE i.id < o.id)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of a cast correlated count subquery counts every outer row
      When query
        """
        SELECT count(CAST((SELECT count(*) FROM range(2) i WHERE i.id < o.id) AS STRING)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of an arithmetic expression over a correlated count subquery counts every outer row
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE i.id < o.id) + 1) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of an ordered limited correlated subquery counts every outer row
      When query
        """
        SELECT count((SELECT o.id + x AS v FROM VALUES (5), (7) AS t(x) ORDER BY v DESC LIMIT 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of an outer reference ordered by its alias with LIMIT counts every outer row
      When query
        """
        SELECT count((SELECT o.id AS v FROM range(1) ORDER BY v LIMIT 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    # DataFusion cannot decorrelate a non-equality correlated predicate under an aggregate.
    @sail-bug
    Scenario: count of a correlated max subquery that matches nothing is zero
      When query
        """
        SELECT count((SELECT max(x) FROM VALUES (5), (7) AS t(x) WHERE x > o.id + 10)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 0 |

  Rule: Uncorrelated scalar subqueries inside aggregate arguments

    Scenario: count of a limited uncorrelated subquery counts every outer row
      When query
        """
        SELECT count((SELECT x FROM VALUES (5), (7) AS t(x) LIMIT 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of a multi-row uncorrelated subquery errors
      When query
        """
        SELECT count((SELECT x FROM VALUES (5), (7) AS t(x))) AS n FROM range(2) o
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

  Rule: Correlated scalar subqueries keyed by a computed inner value

    Scenario: count of a correlated count subquery keyed by a cast inner value counts every outer row
      When query
        """
        SELECT count((SELECT count(*) FROM VALUES ('1'), ('01') AS i(k) WHERE CAST(i.k AS BIGINT) = o.id)) AS n
        FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: a correlated count subquery keyed by a cast inner value counts matches per outer row
      When query
        """
        SELECT o.id, (SELECT count(*) FROM VALUES ('1'), ('01') AS i(k) WHERE CAST(i.k AS BIGINT) = o.id) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 2 |

    Scenario: a filter on a correlated count subquery keyed by a cast inner value keeps matching outer rows
      When query
        """
        SELECT o.id FROM range(2) o
        WHERE (SELECT count(*) FROM VALUES ('1'), ('01') AS i(k) WHERE CAST(i.k AS BIGINT) = o.id) = 2
        """
      Then query result collected
        | id |
        | 1  |

    Scenario: a correlated max subquery keyed by a cast inner value on the right side yields NULL without a match
      When query
        """
        SELECT o.id, (SELECT max(i.k) FROM VALUES ('1'), ('01') AS i(k) WHERE o.id = CAST(i.k AS BIGINT)) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | 1    |

    Scenario: count of a correlated count subquery short-circuits an invalid cast behind an outer predicate
      When query
        """
        SELECT count((SELECT count(*) FROM VALUES ('bad') AS i(k) WHERE o.id = 0 OR CAST(k AS BIGINT) = o.id)) AS n
        FROM range(1) o
        """
      Then query result collected
        | n |
        | 1 |

  Rule: Correlated aggregate subqueries with limits and non-equality predicates

    Scenario: a correlated max subquery with LIMIT yields NULL without a match
      When query
        """
        SELECT o.id, (SELECT max(x) FROM VALUES (5), (7) AS t(x) WHERE x = o.id + 5 LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | 5    |
        | 1  | NULL |

    Scenario: a correlated count subquery with ORDER BY and LIMIT counts matches per outer row
      When query
        """
        SELECT o.id, (SELECT count(*) FROM VALUES (5), (7) AS t(x) WHERE x = o.id + 5 ORDER BY 1 LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 1 |
        | 1  | 0 |

    Scenario: a correlated max subquery filtered only by the outer row yields NULL when the outer predicate fails
      When query
        """
        SELECT o.id, (SELECT max(x) FROM VALUES (5), (7) AS t(x) WHERE o.id >= 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | 7    |

    Scenario: a correlated count subquery filtered only by the outer row counts all inner rows when the outer predicate holds
      When query
        """
        SELECT o.id, (SELECT count(*) FROM VALUES (5), (7) AS t(x) WHERE o.id >= 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 2 |

    Scenario: a correlated max subquery with an OR of correlated and outer-only predicates
      When query
        """
        SELECT o.id, (SELECT max(x) FROM VALUES (5), (7) AS t(x) WHERE x = o.id + 5 OR o.id = 2) AS c
        FROM range(3) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | 5    |
        | 1  | NULL |
        | 2  | 7    |

  Rule: Correlated scalar subqueries without aggregation

    Scenario: count of an outer reference projected through a single-row subquery counts every outer row
      When query
        """
        SELECT count((SELECT o.id FROM range(1))) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of an unaggregated correlated subquery with no matches is zero
      When query
        """
        SELECT count((SELECT i.id FROM range(1) i WHERE i.id = o.id + 5)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 0 |

    Scenario: an unaggregated correlated subquery with one match per outer row is nullable
      When query
        """
        SELECT o.id, (SELECT i.id FROM range(3) i WHERE i.id = o.id) AS c FROM range(2) o ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 1 |
      Then query schema
        """
        root
         |-- id: long (nullable = false)
         |-- c: long (nullable = true)
        """

    Scenario: a filter on an unaggregated correlated subquery being NULL keeps unmatched outer rows
      When query
        """
        SELECT o.id FROM range(3) o WHERE (SELECT i.id FROM range(2) i WHERE i.id = o.id) IS NULL
        """
      Then query result collected
        | id |
        | 2  |

    Scenario: an unaggregated correlated subquery with LIMIT yields the matched row
      When query
        """
        SELECT o.id, (SELECT i.id FROM range(3) i WHERE i.id = o.id LIMIT 1) AS c FROM range(2) o ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 1 |

    Scenario: an unaggregated correlated subquery with multiple matches errors
      When query
        """
        SELECT (SELECT i.k FROM VALUES (1, 4), (1, 2) AS i(k, x) WHERE i.k = o.k) AS c FROM VALUES (1) AS o(k)
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

    Scenario: count of an unaggregated correlated subquery with multiple matches errors
      When query
        """
        SELECT count((SELECT i.k FROM VALUES (1, 4), (1, 2) AS i(k, x) WHERE i.k = o.k)) AS n FROM VALUES (1) AS o(k)
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

    Scenario: count of an outer reference projected through a subquery with LIMIT counts every outer row
      When query
        """
        SELECT count((SELECT o.id FROM range(1) LIMIT 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of an outer reference projected through a subquery with ORDER BY counts every outer row
      When query
        """
        SELECT count((SELECT o.id FROM range(1) ORDER BY id)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: count of a correlated subquery with ORDER BY but no LIMIT counts every match
      When query
        """
        SELECT count((SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id ORDER BY t.x)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    Scenario: a correlated subquery with ORDER BY but no LIMIT returns its single match
      When query
        """
        SELECT o.id, (SELECT o.id + t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id ORDER BY t.x) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 2 |

    Scenario: an outer reference combined with the top row of an ordered limited subquery
      When query
        """
        SELECT o.id, (SELECT o.id + x FROM VALUES (5), (7) AS t(x) ORDER BY x DESC LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 7 |
        | 1  | 8 |

    # The sort key `v` mixes the outer `o.id` with the inner `x`; evaluating it below the row
    # selection would ignore overflow, so Sail declines the rewrite and errors.
    @sail-bug
    Scenario: an outer reference subtracted in the top row of an ordered limited subquery
      When query
        """
        SELECT o.id, (SELECT o.id - x AS v FROM VALUES (5), (7) AS t(x) ORDER BY v DESC LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c  |
        | 0  | -5 |
        | 1  | -4 |

    Scenario: count of an outer reference projected through a multi-row subquery errors
      When query
        """
        SELECT count((SELECT o.id FROM range(2) i)) AS n FROM range(1) o
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)

    Scenario: an unaggregated correlated subquery with ORDER BY DESC and LIMIT yields NULL without a match
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7) AS t(x) WHERE x = o.id + 5 ORDER BY x DESC LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | 5    |
        | 1  | NULL |

  Rule: Row bounds compose inside scalar subqueries

    Scenario: nested ordered limits inside a correlated subquery compose
      When query
        """
        SELECT (
          SELECT v FROM (SELECT o.id + x AS v, x FROM VALUES (0), (1), (2) AS t(x) ORDER BY x ASC LIMIT 2)
          ORDER BY x DESC LIMIT 1
        ) AS result
        FROM range(1) o
        """
      Then query result collected
        | result |
        | 1      |

    Scenario: a volatile sort with LIMIT inside a correlated subquery keeps rows consistent
      When query
        """
        SELECT (
          SELECT y = x + 1L AND o.id = o.id FROM (SELECT id AS x, id + 1L AS y FROM range(100)) q
          ORDER BY rand() LIMIT 1
        ) AS ok
        FROM range(2) o
        """
      Then query result collected
        | ok   |
        | true |
        | true |

    # DataFusion cannot decorrelate a non-equality correlated predicate below the row-bound producer.
    @sail-bug
    Scenario: a volatile sort with LIMIT below a correlated filter keeps rows consistent
      When query
        """
        SELECT (
          SELECT y = x + 1L AND o.id = o.id FROM (SELECT id AS x, id + 1L AS y FROM range(100)) q
          WHERE x >= o.id - o.id
          ORDER BY rand() LIMIT 1
        ) AS ok
        FROM range(2) o
        """
      Then query result collected
        | ok   |
        | true |
        | true |

    Scenario: an outer reference combined with the second row of an ordered subquery via OFFSET
      When query
        """
        SELECT o.id, (SELECT o.id + x FROM VALUES (5), (7) AS t(x) ORDER BY x DESC LIMIT 1 OFFSET 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 5 |
        | 1  | 6 |

    Scenario: count of an outer reference projected through a subquery with LIMIT 0 is zero
      When query
        """
        SELECT count((SELECT o.id FROM range(1) LIMIT 0)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 0 |

    Scenario: count of an outer reference projected through a subquery with LIMIT and OFFSET counts every outer row
      When query
        """
        SELECT count((SELECT o.id FROM range(2) LIMIT 1 OFFSET 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    # DataFusion cannot decorrelate a non-equality correlated predicate below the row-bound producer.
    @sail-bug
    Scenario: a correlated filter with ORDER BY, LIMIT and OFFSET yields the second matching row
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7), (9) AS t(x) WHERE x > o.id ORDER BY x LIMIT 1 OFFSET 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 7 |
        | 1  | 7 |

    Scenario: a volatile sort with LIMIT below an equality-correlated filter keeps rows consistent
      When query
        """
        SELECT (SELECT y = x + 1L AND o.id = o.id FROM (SELECT id AS x, id + 1L AS y FROM range(100)) q WHERE x % 2 = o.id ORDER BY rand() LIMIT 1) AS ok
        FROM range(2) o
        """
      Then query result collected
        | ok   |
        | true |
        | true |

    Scenario: an equality-correlated filter with ORDER BY, LIMIT and OFFSET yields the second matching row
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7), (9) AS t(x) WHERE x % 2 = o.id ORDER BY x LIMIT 1 OFFSET 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | 7    |

    Scenario: count of an equality-correlated subquery with ORDER BY, LIMIT and OFFSET counts the outer rows with a second match
      When query
        """
        SELECT count((SELECT x FROM VALUES (5), (7), (9) AS t(x) WHERE x % 2 = o.id ORDER BY x LIMIT 1 OFFSET 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 1 |

    Scenario: an equality-correlated filter with ORDER BY and OFFSET but no LIMIT yields the remaining match
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7) AS t(x) WHERE x % 2 = o.id ORDER BY x OFFSET 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | 7    |

    Scenario: an equality-correlated filter with OFFSET that leaves two rows errors
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7), (9) AS t(x) WHERE x % 2 = o.id ORDER BY x OFFSET 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query error (?i)(SCALAR_SUBQUERY_TOO_MANY_ROWS|more than one row)


    Scenario: a correlated filter with LIMIT 0 yields NULL for every outer row
      When query
        """
        SELECT o.id, (SELECT x FROM VALUES (5), (7) AS t(x) WHERE x > o.id LIMIT 0) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | NULL |

  Rule: Inlined subqueries evaluate their input once

    Scenario: a random inner value compared with itself inside a correlated subquery is stable
      When query
        """
        SELECT (SELECT x = x AND o.id = o.id FROM (SELECT rand() AS x) i) AS ok FROM range(2) o
        """
      Then query result collected
        | ok   |
        | true |
        | true |

    Scenario: a uuid projected twice inside a correlated subquery compares equal
      When query
        """
        SELECT (SELECT CASE WHEN x = y THEN o.id ELSE -1L END FROM (SELECT u AS x, u AS y FROM (SELECT uuid() AS u)) q) AS result
        FROM range(1) o
        """
      Then query result collected
        | result |
        | 0      |

  Rule: Outer references in a correlated aggregate select list

    # Spark rejects outer references in an aggregate's select list (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE); Sail evaluates them.
    @sail-only
    Scenario: an outer reference added to a correlated aggregate result
      When query
        """
        SELECT o.id, (SELECT max(x) + o.id FROM VALUES (5), (7) AS t(x)) AS c FROM range(2) o ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 7 |
        | 1  | 8 |

    # Spark rejects outer references in an aggregate's select list (UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE); Sail evaluates them.
    @sail-only
    Scenario: an outer reference added to an empty correlated aggregate result is NULL
      When query
        """
        SELECT o.id, (SELECT max(x) + o.id FROM VALUES (5) AS t(x) WHERE false) AS c FROM range(2) o ORDER BY o.id
        """
      Then query result collected ordered
        | id | c    |
        | 0  | NULL |
        | 1  | NULL |

  Rule: A correlated LIMIT or ORDER BY is evaluated per outer row, an intentional divergence from Spark 4.2

    # Sail deliberately diverges from JVM Spark 4.2 here, and the Sail answers are the correct ones:
    # do not change these scenarios to match Spark. Spark folds `LIMIT n` over a literal VALUES
    # relation into its first n rows (`ConvertToLocalRelation`, optimizer/Optimizer.scala) after the
    # correlated predicate has been pulled out of the subquery, so the limit runs before the per-row
    # filter and one outer row wrongly gets NULL (and `count` wrongly gives 1). Spark returns the Sail
    # answers for the same queries over `range(2) t` instead of `VALUES`, and DuckDB agrees with Sail
    # on every shape. Spark also loses the DESC order of `ORDER BY .. LIMIT 1` under an outer-only
    # predicate. The scenarios are `@sail-only` because JVM Spark fails them.

    @sail-only
    Scenario: a correlated LIMIT over literal rows applies after the per-row filter
      When query
        """
        SELECT o.id, (SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 1 |

    @sail-only
    Scenario: a correlated ORDER BY with LIMIT over literal rows applies after the per-row filter
      When query
        """
        SELECT o.id, (SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id ORDER BY t.x DESC LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 1 |

    @sail-only
    Scenario: an outer reference combined with a correlated LIMIT over literal rows
      When query
        """
        SELECT o.id, (SELECT o.id + t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 0 |
        | 1  | 2 |

    @sail-only
    Scenario: count of a correlated LIMIT over literal rows counts every outer row with a match
      When query
        """
        SELECT count((SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id LIMIT 1)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 2 |

    @sail-only
    Scenario: a correlated LIMIT over literal rows is not NULL for an outer row with a match
      When query
        """
        SELECT o.id FROM range(2) o WHERE (SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE t.x = o.id LIMIT 1) IS NULL
        """
      Then query result collected
        | id |

    @sail-only
    Scenario: an outer-only predicate keeps the descending order of a limited subquery
      When query
        """
        SELECT o.id, (SELECT t.x FROM VALUES (0), (1) AS t(x) WHERE o.id >= 0 ORDER BY t.x DESC LIMIT 1) AS c
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | c |
        | 0  | 1 |
        | 1  | 1 |

  Rule: LIMIT, ORDER BY and null checks over a correlated subquery keep their semantics

    Scenario: LIMIT 0 above a correlated aggregate yields NULL
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(2) i WHERE i.id = o.id LIMIT 0) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | NULL |
        | 1  | NULL |

    Scenario: OFFSET above a correlated aggregate skips its single row
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(2) i WHERE i.id = o.id LIMIT 1 OFFSET 1) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n    |
        | 0  | NULL |
        | 1  | NULL |

    Scenario: count of a correlated aggregate under LIMIT 0 is zero
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE i.id = o.id LIMIT 0)) AS n FROM range(2) o
        """
      Then query result collected
        | n |
        | 0 |

    Scenario: ORDER BY with LIMIT 1 above a correlated aggregate keeps its single row
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(2) i WHERE i.id = o.id ORDER BY 1 LIMIT 1) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 1 |
        | 1  | 1 |

    # The sort key mixes the outer `o.id` with the inner `x`. Sorting by `x` alone would hide the
    # overflow, so Sail declines the rewrite and errors instead of raising the overflow itself.
    @sail-bug
    Scenario: an overflowing sort key that mixes outer and inner columns raises
      When query
        """
        SELECT (SELECT o.id + x AS v FROM VALUES (9223372036854775807L), (0L) AS t(x) ORDER BY v ASC LIMIT 1) AS v
        FROM range(1, 2) o
        """
      Then query error (?i)(ARITHMETIC_OVERFLOW|overflow)

    Scenario Outline: a null check on a correlated aggregate with an invalid cast raises the cast error
      When query
        """
        SELECT <expression> AS result FROM range(2) o
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

      Examples:
        | expression                                                                             |
        | (SELECT CAST('bad' AS INT) + count(*) FROM range(1) i WHERE i.id = o.id) IS NULL       |
        | (SELECT CAST('bad' AS INT) + count(*) FROM range(1) i WHERE i.id = o.id) IS NOT NULL   |
        | isnull((SELECT CAST('bad' AS INT) + count(*) FROM range(1) i WHERE i.id = o.id))       |

    Scenario: a volatile projected value is sorted and returned as one value under correlation
      When query
        """
        SELECT count(*) AS high
        FROM (SELECT (SELECT rand() AS v FROM range(10000) i WHERE i.id % 10 = o.id ORDER BY v ASC LIMIT 1) AS v FROM range(10) o) q
        WHERE v > 0.1
        """
      Then query result collected
        | high |
        | 0    |

    Scenario: a volatile projected value is sorted and returned as one value
      When query
        """
        SELECT (SELECT rand() AS v FROM range(10000) ORDER BY v LIMIT 1) < 0.01 AS low
        """
      Then query result collected
        | low  |
        | true |

    Scenario: the top of a volatile projected value under correlation is its maximum
      When query
        """
        SELECT o.id, (SELECT rand() AS v FROM range(10000) i WHERE i.id % 10 = o.id ORDER BY v DESC LIMIT 1) > 0.9 AS high
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | high |
        | 0  | true |
        | 1  | true |

  Rule: A subquery DataFusion can decorrelate is evaluated, so its errors surface

    # Only a subquery that DataFusion cannot plan (a non-equality correlation) keeps the
    # pre-nullability shortcut of never being evaluated; every other one runs.

    Scenario Outline: a null check on a correlated aggregate whose predicate needs a failing implicit cast raises
      When query
        """
        SELECT <expression> AS result FROM range(2) o
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

      Examples:
        | expression                                                                        |
        | (SELECT count(*) FROM range(1) i WHERE i.id = o.id AND i.id = 'bad') IS NULL      |
        | (SELECT count(*) FROM range(1) i WHERE i.id = o.id AND i.id = 'bad') IS NOT NULL  |
        | (SELECT count(*) = 'bad' FROM range(1) i WHERE i.id = o.id) IS NULL               |

    # COUNT never evaluates a provably non-null single-row argument (as before this fix), so an
    # error inside that subquery is not raised.
    @sail-bug
    Scenario: count of a correlated aggregate whose correlation key fails to cast raises
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE CAST(concat('bad', i.id) AS BIGINT) = o.id)) AS n FROM range(2) o
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

    # COUNT never evaluates a provably non-null single-row argument (as before this fix), so an
    # error inside that subquery is not raised.
    @sail-bug
    Scenario: count of a correlated aggregate whose correlation key divides by zero raises
      When query
        """
        SELECT count((SELECT count(*) FROM range(2) i WHERE 1 / (i.id - i.id) = o.id)) AS n FROM range(2) o
        """
      Then query error (?i)(DIVIDE_BY_ZERO|division by zero)

    Scenario: two conjuncts with outer-only disjuncts are both hoisted
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(2) i WHERE (o.id = 0 OR i.id = 1) AND (o.id = 1 OR i.id = 0)) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 1 |
        | 1  | 1 |

    Scenario Outline: a null check on a non-equality correlated aggregate folds as it did before
      When query
        """
        SELECT <expression> AS result FROM range(2) o
        """
      Then query result collected
        | result |
        | false  |
        | false  |

      Examples:
        | expression                                                                 |
        | (SELECT count(*) + 1 FROM range(2) i WHERE i.id < o.id) IS NULL            |
        | (SELECT CAST(count(*) AS STRING) FROM range(2) i WHERE i.id < o.id) IS NULL |

    # A volatile sort key that also references the outer row cannot be evaluated below the row
    # selection and is not constant per outer row either, so Sail declines the rewrite and errors.
    @sail-bug
    Scenario: the minimum of a volatile key shifted by an outer reference
      When query
        """
        SELECT (SELECT rand() + o.id AS v FROM range(1000) i ORDER BY v LIMIT 1) - o.id < 0.05 AS low FROM range(1) o
        """
      Then query result collected
        | low  |
        | true |

    @sail-bug
    Scenario: the maximum of a volatile key shifted by an outer reference
      When query
        """
        SELECT (SELECT rand() + o.id AS v FROM range(1000) i ORDER BY v DESC LIMIT 1) - o.id > 0.95 AS high FROM range(1) o
        """
      Then query result collected
        | high |
        | true |

  Rule: A null check on a subquery DataFusion cannot plan folds only after the subquery is planned

    # The fold runs after DataFusion has analyzed and simplified the subquery, so an invalid
    # constant cast inside it still raises. A subquery that folds is not evaluated, so an error
    # it would only raise at execution is not surfaced; that is inherited from before scalar
    # subqueries were nullable, when DataFusion folded such checks itself.

    Scenario: a constant invalid cast inside a non-equality correlated aggregate under a null check raises
      When query
        """
        SELECT (SELECT CAST('bad' AS INT) + count(*) FROM range(1) i WHERE i.id < o.id) IS NULL AS result FROM range(2) o
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

    Scenario: a guarded null check folds without evaluating the guarded branch
      When query
        """
        SELECT (SELECT count(*) FROM VALUES ('bad') t(k) WHERE o.id = 0 OR CAST(k AS BIGINT) = o.id) IS NULL AS n FROM range(1) o
        """
      Then query result collected
        | n     |
        | false |

    # Inherited: the subquery is never evaluated because DataFusion cannot plan it, so the
    # per-row cast error Spark raises is not surfaced.
    @sail-bug
    Scenario: a runtime cast error inside a non-equality correlated aggregate under a null check is not raised
      When query
        """
        SELECT (SELECT count(*) FROM range(2) i WHERE i.id < o.id AND CAST(concat('bad', i.id) AS BIGINT) = o.id) IS NULL AS n FROM range(2) o
        """
      Then query error (?i)(CAST_INVALID_INPUT|cannot (be )?cast|invalid|NumberFormat|parse)

  Rule: Outer-only guards are hoisted together

    Scenario: many predicates on the outer row alone are hoisted as one guard
      When query
        """
        SELECT (SELECT count(*) FROM range(1) i WHERE o.id >= 0 AND o.id >= 1 AND o.id >= 2 AND o.id >= 3 AND o.id >= 4 AND o.id >= 5 AND o.id >= 6 AND o.id >= 7 AND o.id >= 8 AND o.id >= 9 AND o.id >= 10 AND o.id >= 11 AND o.id >= 12 AND o.id >= 13) AS n
        FROM range(1) o
        """
      Then query result collected
        | n |
        | 0 |

    Scenario: four OR guards mixing outer and inner columns are hoisted
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(4) i WHERE (o.id = 0 OR i.id = 1) AND (o.id = 1 OR i.id = 1) AND (o.id = 2 OR i.id = 1) AND (o.id = 3 OR i.id = 1)) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 1 |
        | 1  | 1 |

    # Support limit: each OR guard doubles the number of subquery plans, so Sail hoists at most
    # four of them and otherwise declines the rewrite, which DataFusion then cannot plan.
    @sail-bug
    Scenario: more than four OR guards are not hoisted
      When query
        """
        SELECT o.id, (SELECT count(*) FROM range(4) i WHERE (o.id = 0 OR i.id = 1) AND (o.id = 1 OR i.id = 1) AND (o.id = 2 OR i.id = 1) AND (o.id = 3 OR i.id = 1) AND (o.id = 4 OR i.id = 1)) AS n
        FROM range(2) o
        ORDER BY o.id
        """
      Then query result collected ordered
        | id | n |
        | 0  | 1 |
        | 1  | 1 |
