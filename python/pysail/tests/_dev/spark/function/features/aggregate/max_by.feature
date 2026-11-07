Feature: max_by function

  Rule: max_by with all NULLs in ordering column

    Scenario: max_by with all NULLs in ordering column
      When query
        """
        SELECT max_by(name, age) AS result
        FROM VALUES ('Alice', CAST(NULL AS INT)), ('Bob', CAST(NULL AS INT)) AS t(name, age)
        """
      Then query result
        | result |
        | NULL   |

  Rule: max_by as window function

    Scenario: max_by over window
      When query
        """
        SELECT name, age,
               max_by(name, age) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('Alice', 30), ('Bob', 50), ('Carol', 40) AS t(name, age)
        ORDER BY age
        """
      Then query result ordered
        | name  | age | result |
        | Alice | 30  | Alice  |
        | Carol | 40  | Carol  |
        | Bob   | 50  | Bob    |

  Rule: Result values (migrated from test_max_by.txt doctests)

    # All four doctests share the same alltypes/t_base fixture and differ only in
    # what they select from, so the fixture is written once and <source> varies.
    Scenario Outline: max_by doctest <case> (result)
      When query
        """
        WITH alltypes AS (SELECT CAST(v AS INT) AS int_col, CAST(v % 3 AS BIGINT) AS bigint_col FROM (SELECT explode(sequence(0, 9)) AS v)), t_base AS (SELECT int_col, bigint_col, CASE WHEN int_col IN (0, 9) THEN NULL ELSE int_col END AS by_col, int_col * 10 AS val_col FROM alltypes) SELECT max_by(val_col, by_col) AS result FROM <source>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case | source                                                                                                                | result |
        | #1   | t_base                                                                                                                | 80     |
        | #2   | (SELECT CASE WHEN val_col = 80 THEN NULL ELSE val_col END AS val_col, by_col FROM t_base)                             | NULL   |
        | #3   | t_base WHERE int_col <> 8                                                                                             | 70     |
        | #4   | (SELECT CASE WHEN val_col = 70 THEN NULL ELSE val_col END AS val_col, by_col, int_col FROM t_base) WHERE int_col <> 8 | NULL   |

  Rule: Nested orderings with NULL fields or elements

    Scenario: max_by treats a NULL struct field as the smallest
      When query
        """
        SELECT max_by(v, k) AS result FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k)
        """
      Then query result
        | result |
        | 3      |

    Scenario: max_by treats a NULL array element as the smallest
      When query
        """
        SELECT max_by(v, k) AS result FROM VALUES (1, array(CAST(NULL AS INT), 5)), (2, array(1, 1)), (3, array(2, 0)) AS t(v, k)
        """
      Then query result
        | result |
        | 3      |

    Scenario: max_by treats a NULL field of a struct in an array as the smallest
      When query
        """
        SELECT max_by(v, k) AS result FROM VALUES (1, array(named_struct('a', CAST(NULL AS INT)))), (2, array(named_struct('a', 1))), (3, array(named_struct('a', 2))) AS t(v, k)
        """
      Then query result
        | result |
        | 3      |

    Scenario: max_by treats a NULL nested struct as the smallest field
      When query
        """
        SELECT max_by(v, k) AS result FROM VALUES (1, named_struct('s', CAST(NULL AS STRUCT<a: INT>))), (2, named_struct('s', named_struct('a', 1))), (3, CAST(NULL AS STRUCT<s: STRUCT<a: INT>>)) AS t(v, k)
        """
      Then query result
        | result |
        | 2      |

    Scenario: max_by orders arrays by elements before length
      When query
        """
        SELECT max_by(v, k) AS result FROM VALUES (1, array(1)), (2, array(1, CAST(NULL AS INT))), (3, array(CAST(NULL AS INT))) AS t(v, k)
        """
      Then query result
        | result |
        | 2      |

    Scenario: max_by with rows across batches
      When query
        """
        SELECT max_by(id, named_struct('a', CASE WHEN id < 10000 THEN NULL ELSE 0 END, 'b', -id)) AS s, max_by(id, array(CASE WHEN id < 10000 THEN NULL ELSE 0 END, -id)) AS l, max_by(id, named_struct('a', CASE WHEN id >= 10000 THEN NULL ELSE 0 END, 'b', id)) AS s2, max_by(id, array(CASE WHEN id >= 10000 THEN NULL ELSE 0 END, id)) AS l2 FROM range(0, 20000, 1, 1)
        """
      Then query result
        | s     | l     | s2   | l2   |
        | 10000 | 10000 | 9999 | 9999 |

    Scenario: max_by with rows across batches with a filter
      When query
        """
        SELECT max_by(id, named_struct('a', CASE WHEN id < 10000 THEN NULL ELSE 0 END, 'b', -id)) AS s, max_by(id, array(CASE WHEN id < 10000 THEN NULL ELSE 0 END, -id)) AS l FROM (SELECT id FROM range(0, 20000, 1, 1) WHERE id < 100 OR id = 15000)
        """
      Then query result
        | s     | l     |
        | 15000 | 15000 |

    Scenario: max_by with rows across partitions
      When query
        """
        SELECT max_by(id, named_struct('a', CASE WHEN id < 10000 THEN NULL ELSE 0 END, 'b', -id)) AS s, max_by(id, array(CASE WHEN id < 10000 THEN NULL ELSE 0 END, -id)) AS l FROM range(0, 20000, 1, 4)
        """
      Then query result
        | s     | l     |
        | 10000 | 10000 |

    Scenario: max_by with groups across batches
      When query
        """
        SELECT id % 2 AS g, max_by(id, named_struct('a', CASE WHEN id < 10000 THEN NULL ELSE 0 END, 'b', -id)) AS s, max_by(id, array(CASE WHEN id < 10000 THEN NULL ELSE 0 END, -id)) AS l FROM range(0, 20000, 1, 1) GROUP BY 1
        """
      Then query result
        | g | s     | l     |
        | 0 | 10000 | 10000 |
        | 1 | 10001 | 10001 |

    Scenario: max_by in a window over the whole partition
      When query
        """
        SELECT v, max_by(v, k) OVER () AS s FROM (SELECT * FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k))
        """
      Then query result
        | v | s |
        | 1 | 3 |
        | 2 | 3 |
        | 3 | 3 |

    Scenario: max_by in a window over the whole partition with an array of structs
      When query
        """
        SELECT v, max_by(v, k) OVER () AS s FROM (SELECT * FROM VALUES (1, array(named_struct('a', CAST(NULL AS INT)))), (2, array(named_struct('a', 1))), (3, array(named_struct('a', 2))) AS t(v, k))
        """
      Then query result
        | v | s |
        | 1 | 3 |
        | 2 | 3 |
        | 3 | 3 |

    Scenario: max_by in a window with a growing frame
      When query
        """
        SELECT v, max_by(v, k) OVER (ORDER BY v) AS s FROM VALUES (1, named_struct('a', CAST(NULL AS INT), 'b', 5)), (2, named_struct('a', 1, 'b', 1)), (3, named_struct('a', 2, 'b', 0)) AS t(v, k)
        """
      Then query result
        | v | s |
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
