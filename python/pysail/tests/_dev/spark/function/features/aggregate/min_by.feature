Feature: min_by function

  Rule: min_by with all NULLs in ordering column

    Scenario: min_by with all NULLs in ordering column
      When query
        """
        SELECT min_by(name, age) AS result
        FROM VALUES ('Alice', CAST(NULL AS INT)), ('Bob', CAST(NULL AS INT)) AS t(name, age)
        """
      Then query result
        | result |
        | NULL   |

  Rule: min_by as window function

    Scenario: min_by over window
      When query
        """
        SELECT name, age,
               min_by(name, age) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('Alice', 30), ('Bob', 50), ('Carol', 40) AS t(name, age)
        ORDER BY age
        """
      Then query result ordered
        | name  | age | result |
        | Alice | 30  | Alice  |
        | Carol | 40  | Alice  |
        | Bob   | 50  | Alice  |

  Rule: Result values (migrated from test_min_by.txt doctests)

    Scenario: min_by doctest #1 (result)
      When query
        """
        WITH alltypes AS (SELECT CAST(v AS INT) AS int_col, CAST(v % 3 AS BIGINT) AS bigint_col FROM (SELECT explode(sequence(0, 9)) AS v)), t_base AS (SELECT int_col, bigint_col, CASE WHEN int_col IN (0, 9) THEN NULL ELSE int_col END AS by_col, int_col * 10 AS val_col FROM alltypes) SELECT * FROM t_base
        """
      Then query result
        | int_col | bigint_col | by_col | val_col |
        | 0       | 0          | NULL   | 0       |
        | 1       | 1          | 1      | 10      |
        | 2       | 2          | 2      | 20      |
        | 3       | 0          | 3      | 30      |
        | 4       | 1          | 4      | 40      |
        | 5       | 2          | 5      | 50      |
        | 6       | 0          | 6      | 60      |
        | 7       | 1          | 7      | 70      |
        | 8       | 2          | 8      | 80      |
        | 9       | 0          | NULL   | 90      |

    # Doctests #2, #4 and #5 share the alltypes/t_base fixture and the same
    # min_by(val_col, by_col) call, differing only in what they select from.
    Scenario Outline: min_by doctest <case> (result)
      When query
        """
        WITH alltypes AS (SELECT CAST(v AS INT) AS int_col, CAST(v % 3 AS BIGINT) AS bigint_col FROM (SELECT explode(sequence(0, 9)) AS v)), t_base AS (SELECT int_col, bigint_col, CASE WHEN int_col IN (0, 9) THEN NULL ELSE int_col END AS by_col, int_col * 10 AS val_col FROM alltypes) SELECT min_by(val_col, by_col) AS result FROM <source>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case | source                                                                                                                | result |
        | #2   | t_base                                                                                                                | 10     |
        | #4   | t_base WHERE int_col <> 1                                                                                             | 20     |
        | #5   | (SELECT CASE WHEN val_col = 20 THEN NULL ELSE val_col END AS val_col, by_col, int_col FROM t_base) WHERE int_col <> 1 | NULL   |

    Scenario: min_by doctest #3 (result)
      When query
        """
        WITH alltypes AS (SELECT CAST(v AS INT) AS int_col, CAST(v % 3 AS BIGINT) AS bigint_col FROM (SELECT explode(sequence(0, 9)) AS v)), t_base AS (SELECT int_col, bigint_col, CASE WHEN int_col IN (0, 9) THEN NULL ELSE int_col END AS by_col, int_col * 10 AS val_col FROM alltypes) SELECT min_by(val_col, CASE WHEN by_col IS NULL THEN by_col ELSE by_col END) AS result FROM (SELECT CASE WHEN val_col = 10 THEN NULL ELSE val_col END AS val_col, by_col FROM t_base)
        """
      Then query result
        | result |
        | NULL   |

  Rule: Nested orderings with NULL fields or elements

    Scenario: min_by treats a NULL struct field as the smallest
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k)
        """
      Then query result
        | result |
        | 1      |

    Scenario: min_by treats a NULL array element as the smallest
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, array(CAST(NULL AS INT), 5)), (2, array(1, 1)), (3, array(2, 0)) AS t(v, k)
        """
      Then query result
        | result |
        | 1      |

    Scenario: min_by treats a NULL field of a struct in an array as the smallest
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, array(named_struct('a', CAST(NULL AS INT)))), (2, array(named_struct('a', 1))), (3, array(named_struct('a', 2))) AS t(v, k)
        """
      Then query result
        | result |
        | 1      |

    Scenario: min_by treats a NULL nested struct as the smallest field
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, named_struct('s', CAST(NULL AS STRUCT<a: INT>))), (2, named_struct('s', named_struct('a', 1))), (3, CAST(NULL AS STRUCT<s: STRUCT<a: INT>>)) AS t(v, k)
        """
      Then query result
        | result |
        | 1      |

    Scenario: min_by treats a NULL nested array as the smallest element
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, array(CAST(NULL AS ARRAY<INT>))), (2, array(array(1))), (3, array(array(CAST(NULL AS INT)))) AS t(v, k)
        """
      Then query result
        | result |
        | 1      |

    Scenario: min_by orders arrays by elements before length
      When query
        """
        SELECT min_by(v, k) AS result FROM VALUES (1, array(1)), (2, array(1, CAST(NULL AS INT))), (3, array(CAST(NULL AS INT))) AS t(v, k)
        """
      Then query result
        | result |
        | 3      |

    Scenario: min_by with groups and a NULL struct field
      When query
        """
        SELECT v % 2 AS g, min_by(v, k) AS result FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k) GROUP BY 1
        """
      Then query result
        | g | result |
        | 0 | 2      |
        | 1 | 1      |

    Scenario: min_by with rows across batches
      When query
        """
        SELECT min_by(id, named_struct('a', CASE WHEN id >= 10000 THEN NULL ELSE 0 END, 'b', id)) AS s, min_by(id, array(CASE WHEN id >= 10000 THEN NULL ELSE 0 END, id)) AS l, min_by(id, named_struct('a', CASE WHEN id < 10000 THEN NULL ELSE 0 END, 'b', -id)) AS s2, min_by(id, array(CASE WHEN id < 10000 THEN NULL ELSE 0 END, -id)) AS l2 FROM range(0, 20000, 1, 1)
        """
      Then query result
        | s     | l     | s2   | l2   |
        | 10000 | 10000 | 9999 | 9999 |

    Scenario: min_by with rows across batches with a filter
      When query
        """
        SELECT min_by(id, named_struct('a', CASE WHEN id >= 10000 THEN NULL ELSE 0 END, 'b', id)) AS s, min_by(id, array(CASE WHEN id >= 10000 THEN NULL ELSE 0 END, id)) AS l FROM (SELECT id FROM range(0, 20000, 1, 1) WHERE id < 100 OR id = 15000)
        """
      Then query result
        | s     | l     |
        | 15000 | 15000 |

    Scenario: min_by with rows across partitions
      When query
        """
        SELECT min_by(id, named_struct('a', CASE WHEN id >= 10000 THEN NULL ELSE 0 END, 'b', id)) AS s, min_by(id, array(CASE WHEN id >= 10000 THEN NULL ELSE 0 END, id)) AS l FROM range(0, 20000, 1, 4)
        """
      Then query result
        | s     | l     |
        | 10000 | 10000 |

    Scenario: min_by with groups across batches
      When query
        """
        SELECT id % 2 AS g, min_by(id, named_struct('a', CASE WHEN id >= 10000 THEN NULL ELSE 0 END, 'b', id)) AS s, min_by(id, array(CASE WHEN id >= 10000 THEN NULL ELSE 0 END, id)) AS l FROM range(0, 20000, 1, 1) GROUP BY 1
        """
      Then query result
        | g | s     | l     |
        | 0 | 10000 | 10000 |
        | 1 | 10001 | 10001 |

    Scenario: min_by in a window over the whole partition
      When query
        """
        SELECT v, min_by(v, k) OVER () AS s FROM (SELECT * FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k))
        """
      Then query result
        | v | s |
        | 1 | 1 |
        | 2 | 1 |
        | 3 | 1 |

    Scenario: min_by in a window over the whole partition with an array of structs
      When query
        """
        SELECT v, min_by(v, k) OVER () AS s FROM (SELECT * FROM VALUES (1, array(named_struct('a', CAST(NULL AS INT)))), (2, array(named_struct('a', 1))), (3, array(named_struct('a', 2))) AS t(v, k))
        """
      Then query result
        | v | s |
        | 1 | 1 |
        | 2 | 1 |
        | 3 | 1 |

    Scenario: min_by in a window with a growing frame
      When query
        """
        SELECT v, min_by(v, k) OVER (ORDER BY v) AS s FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k)
        """
      Then query result
        | v | s |
        | 1 | 1 |
        | 2 | 1 |
        | 3 | 1 |

    Scenario: min_by and max_by with a NULL integer field and extreme integers
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS INT))), (2, named_struct('a', -2147483648)), (3, named_struct('a', 2147483647)) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL string field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS STRING))), (2, named_struct('a', '')), (3, named_struct('a', 'a')) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL decimal field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS DECIMAL(10,2)))), (2, named_struct('a', CAST(-1 AS DECIMAL(10,2)))), (3, named_struct('a', CAST(1 AS DECIMAL(10,2)))) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL binary field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS BINARY))), (2, named_struct('a', X'')), (3, named_struct('a', X'01')) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL date field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS DATE))), (2, named_struct('a', DATE'0001-01-01')), (3, named_struct('a', DATE'2020-01-01')) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL timestamp field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS TIMESTAMP))), (2, named_struct('a', TIMESTAMP'1970-01-01 00:00:00')), (3, named_struct('a', TIMESTAMP'2020-01-01 00:00:00')) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL boolean field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS BOOLEAN))), (2, named_struct('a', false)), (3, named_struct('a', true)) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL double field
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, named_struct('a', CAST(NULL AS DOUBLE))), (2, named_struct('a', CAST('-Infinity' AS DOUBLE))), (3, named_struct('a', CAST('NaN' AS DOUBLE))) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with a NULL bigint in a nested array
      When query
        """
        SELECT min_by(v, k) AS mn, max_by(v, k) AS mx FROM VALUES (1, array(array(CAST(NULL AS BIGINT)))), (2, array(array(CAST('-9223372036854775808' AS BIGINT)))), (3, array(array(CAST(0 AS BIGINT)))) AS t(v, k)
        """
      Then query result
        | mn | mx |
        | 1  | 3  |

    Scenario: min_by and max_by with array of struct orderings across partitions
      When query
        """
        SELECT min_by(id, array(named_struct('a', id % 3, 'b', id))) AS mn, max_by(id, array(named_struct('a', id % 3, 'b', id))) AS mx FROM range(0, 1000, 1, 4)
        """
      Then query result
        | mn | mx  |
        | 0  | 998 |

    Scenario: min_by and max_by with struct of array orderings across partitions
      When query
        """
        SELECT min_by(id, named_struct('a', array(id % 3), 'b', id)) AS mn, max_by(id, named_struct('a', array(id % 3), 'b', id)) AS mx FROM range(0, 1000, 1, 4)
        """
      Then query result
        | mn | mx  |
        | 0  | 998 |
