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

  Rule: An argument count outside the accepted range is rejected

    # `Signature::user_defined` means DataFusion runs no arity check of its own, so
    # every hook reaching for the second argument used to index an empty/short slice
    # and panic -- which kills the gRPC connection instead of reporting an error.
    Scenario: min_by with no arguments is rejected
      When query
        """
        SELECT min_by()
        """
      Then query error (?i)min_by.*requires

    Scenario: min_by with a single argument is rejected
      When query
        """
        SELECT min_by(1)
        """
      Then query error (?i)min_by.*requires

    Scenario: min_by with four arguments is rejected
      When query
        """
        SELECT min_by(x, y, 2, 1) FROM VALUES (1, 2), (3, 4) AS t(x, y)
        """
      Then query error (?i)min_by.*requires

  Rule: The ordering argument must be of an orderable type

    # Same rule as `max_by` (shared `MaxMinBy.checkInputDataTypes`): only the ORDERING
    # argument is type-checked, and MAP is not orderable. See `aggregate/max_by.feature`
    # for the ARRAY/STRUCT cases that show the rule is orderability, not "complex type".
    @sail-bug
    Scenario: min_by rejects a MAP ordering column
      When query
        """
        SELECT min_by(v, o) AS result FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error does not support ordering on type

  Rule: The three-argument form returns the k values at the extreme of the ordering column

    # Added in Spark 4.2 (`MaxMinByK.scala`, `MinByBuilder` accepts [2, 3] arguments):
    # returns an ARRAY of the k values, sorted ascending by the ordering column.
    # The array is joined so that the assertion stays plain SQL; the element order is
    # part of what is asserted, and it is deterministic here because every `y` differs.
    @spark-4.2 @sail-bug
    Scenario: min_by returns the bottom k values as an array
      When query
        """
        SELECT array_join(min_by(x, y, 2), ',') AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | a,c    |
