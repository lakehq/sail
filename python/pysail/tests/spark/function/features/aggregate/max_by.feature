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

    # The check is `checkInputDataTypes`, so it fires at ANALYSIS regardless of the
    # data: a folded MAP literal is rejected just like a MAP column.
    @sail-bug
    Scenario: max_by rejects a MAP ordering literal
      When query
        """
        SELECT max_by(1, map('a', 1)) AS result
        """
      Then query error does not support ordering on type

    # The window path never reaches `simplify`, so a missing orderability check is
    # SILENT there rather than merely mis-messaged: Sail returns a value.
    @sail-bug
    Scenario: max_by rejects a MAP ordering column in a window frame
      When query
        """
        SELECT max_by(v, o) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('lo', map('a', 1), 1), ('hi', map('b', 2), 2) AS t(v, o, i)
        """
      Then query error does not support ordering on type

  Rule: IGNORE NULLS is not supported

    # `FunctionResolution.applyIgnoreNulls` has no case for MaxBy/MinBy, so it falls
    # through to `functionWithUnsupportedSyntaxError`. This is not just a missing
    # error: `MaxMinBy.updateExpressions` skips NULL ORDERINGS only and may legitimately
    # return a NULL value, whereas Sail forwards the flag to `last_value`, which skips
    # NULL VALUES and answered 'b' where the two-argument semantics say NULL.
    Scenario: max_by rejects IGNORE NULLS
      When query
        """
        SELECT max_by(v, o) IGNORE NULLS AS result
        FROM VALUES (CAST(NULL AS STRING), 3), ('b', 2), ('c', 1) AS t(v, o)
        """
      Then query error does not support IGNORE NULLS

    # `applyIgnoreNulls` runs on the function itself (the Spark stack goes through
    # `WindowExpression.mapChildren`), so the window form is rejected as well.
    Scenario: max_by rejects IGNORE NULLS in a window frame
      When query
        """
        SELECT max_by(v, o) IGNORE NULLS OVER (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (CAST(NULL AS STRING), 3), ('b', 2), ('c', 1) AS t(v, o)
        """
      Then query error does not support IGNORE NULLS

    Scenario: max_by returns a NULL value at the maximum ordering
      When query
        """
        SELECT max_by(v, o) AS result
        FROM VALUES (CAST(NULL AS STRING), 3), ('b', 2), ('c', 1) AS t(v, o)
        """
      Then query result
        | result |
        | NULL   |

  Rule: The output type is the value argument's type

    # `MaxMinBy.dataType` is `valueExpr.dataType` and `nullable` is unconditionally
    # true, so the value type must survive unwidened -- including decimal precision
    # and the inner nullability of a nested value.
    Scenario: max_by preserves a narrow integer value type
      When query
        """
        SELECT max_by(CAST(1 AS TINYINT), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = true)
        """
      Then query result
        | result |
        | 1      |

    Scenario: max_by preserves decimal precision and scale
      When query
        """
        SELECT max_by(CAST(1.5 AS DECIMAL(20,3)), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(20,3) (nullable = true)
        """

    Scenario: max_by preserves the inner nullability of an ARRAY value
      When query
        """
        SELECT max_by(array(1, 2), 1) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: max_by is nullable even over a non-nullable value column
      When query
        """
        SELECT max_by(id, id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    # Sail carries the Spark type identity of GEOMETRY/GEOGRAPHY/UDT in the FIELD
    # METADATA, which `udaf_default_return_field` drops because neither UDAF
    # implements `return_field`. The bare `st_geomfromwkb` call keeps the type, so
    # the downgrade to `binary` is `max_by`'s own.
    @spark-4.2 @sail-bug
    Scenario: max_by preserves the geometry type of the value argument
      When query
        """
        SELECT max_by(st_geomfromwkb(g), i) AS result
        FROM VALUES (X'0101000000000000000000F03F0000000000000040', 1) AS t(g, i)
        """
      Then query schema
        """
        root
         |-- result: geometry (nullable = true)
        """

  Rule: A star argument is expanded before the function is resolved

    # Spark expands `*` into the table's columns first, so `max_by(*)` over a
    # two-column table resolves to `max_by(x, y)`. Sail does not expand a star inside
    # aggregate arguments and reports an arity error instead.
    @sail-bug
    Scenario: max_by expands a star argument
      When query
        """
        SELECT max_by(*) AS result FROM VALUES (1, 2) AS t(x, y)
        """
      Then query result
        | result |
        | 1      |

    # Expanding here yields `max_by(x, x, y)`, i.e. the three-argument top-k form with a
    # non-foldable `k`, which Spark rejects at ANALYSIS. Sail leaks a DataFusion physical
    # planning error ("Physical plan does not support logical expression Wildcard").
    @spark-4.2 @sail-bug
    Scenario: max_by rejects a non-foldable k from an expanded star
      When query
        """
        SELECT max_by(x, *) AS result FROM VALUES (1, 2) AS t(x, y)
        """
      Then query error should be a foldable

  Rule: Other aggregate clauses

    Scenario: max_by composes with FILTER
      When query
        """
        SELECT max_by(v, o) FILTER (WHERE v <> 'x') AS result FROM VALUES ('x', 9), ('b', 2) AS t(v, o)
        """
      Then query result
        | result |
        | b      |

    # Spark de-duplicates the (value, ordering) pair, not the value alone.
    Scenario: max_by with DISTINCT de-duplicates the argument pair
      When query
        """
        SELECT max_by(DISTINCT v, o) AS result FROM VALUES ('a', 1), ('a', 5), ('b', 3) AS t(v, o)
        """
      Then query result
        | result |
        | a      |

    # A sliding frame needs `retract_batch`, which `MaxMinByAccumulator` does not
    # implement, so Sail fails to plan the query outright.
    @sail-bug
    Scenario: max_by supports a sliding window frame
      When query
        """
        SELECT max_by(v, o) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 1), ('b', 2), ('c', 3) AS t(v, o)
        """
      Then query result ordered
        | result |
        | a      |
        | b      |
        | c      |

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

    # The scenario above joins the array, so it asserts a STRING and would stay green
    # against a wrong element type. `MaxMinByK.dataType` is
    # `ArrayType(valueExpr.dataType, containsNull = true)`; pin it separately.
    @spark-4.2 @sail-bug
    Scenario: max_by returns the top k values with an array output type
      When query
        """
        SELECT max_by(x, y, 2) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
