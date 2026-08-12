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

  Rule: An argument count outside the accepted range is rejected

    # `Signature::user_defined` means DataFusion runs no arity check of its own, so
    # every hook reaching for the second argument used to index an empty/short slice
    # and panic -- which kills the gRPC connection instead of reporting an error.
    Scenario: max_by with no arguments is rejected
      When query
        """
        SELECT max_by()
        """
      Then query error (?i)max_by.*requires

    Scenario: max_by with a single argument is rejected
      When query
        """
        SELECT max_by(1)
        """
      Then query error (?i)max_by.*requires

    Scenario: max_by with four arguments is rejected
      When query
        """
        SELECT max_by(x, y, 2, 1) FROM VALUES (1, 2), (3, 4) AS t(x, y)
        """
      Then query error (?i)max_by.*requires

  Rule: The ordering argument must be of an orderable type

    # `MaxMinBy.checkInputDataTypes` only checks the ORDERING argument, via
    # `TypeUtils.checkForOrderingExpr` -> `OrderUtils.isOrderable`; the value argument
    # has no type restriction at all. ARRAY and STRUCT are orderable and must be
    # ACCEPTED -- the rule is orderability, not "complex type". MAP is not orderable.
    @sail-bug
    Scenario: max_by rejects a MAP ordering column
      When query
        """
        SELECT max_by(v, o) AS result FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error does not support ordering on type

    Scenario: max_by accepts an ARRAY ordering column
      When query
        """
        SELECT max_by(v, o) AS result FROM VALUES ('lo', array(1, 2)), ('hi', array(3, 4)) AS t(v, o)
        """
      Then query result
        | result |
        | hi     |

    Scenario: max_by accepts a STRUCT ordering column
      When query
        """
        SELECT max_by(v, o) AS result FROM VALUES ('lo', named_struct('a', 1)), ('hi', named_struct('a', 2)) AS t(v, o)
        """
      Then query result
        | result |
        | hi     |

  Rule: The three-argument form returns the k values at the extreme of the ordering column

    # Added in Spark 4.2 (`MaxMinByK.scala`, `MaxByBuilder` accepts [2, 3] arguments):
    # returns an ARRAY of the k values, sorted descending by the ordering column.
    # The array is joined so that the assertion stays plain SQL; the element order is
    # part of what is asserted, and it is deterministic here because every `y` differs.
    @spark-4.2 @sail-bug
    Scenario: max_by returns the top k values as an array
      When query
        """
        SELECT array_join(max_by(x, y, 2), ',') AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | b,c    |
