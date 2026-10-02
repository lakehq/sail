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

  Rule: The ordering argument must be of an orderable type

    # Spark checks this in analysis: MaxMinBy.checkInputDataTypes delegates to
    # TypeUtils.checkForOrderingExpr, which rejects MAP, VARIANT and any nested
    # type containing one of them. Only the ordering argument is restricted.

    @sail-bug
    Scenario: max_by rejects a MAP ordering column
      When query
        """
        SELECT max_by(v, o) AS result
        FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error (?s)max_by.*does not support ordering on type

    @sail-bug
    Scenario: max_by rejects a MAP ordering literal
      When query
        """
        SELECT max_by(1, map('a', 1)) AS result
        """
      Then query error (?s)max_by.*does not support ordering on type

    @sail-bug
    Scenario: max_by rejects a MAP ordering column as a window function
      When query
        """
        SELECT max_by(v, map('a', i)) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(v, i)
        """
      Then query error (?s)max_by.*does not support ordering on type

    @sail-bug
    @spark-4
    Scenario: max_by rejects a VARIANT ordering column
      When query
        """
        SELECT max_by(x, parse_json(j)) AS result
        FROM VALUES ('a', '"aaa"'), ('b', '"b"') AS t(x, j)
        """
      Then query error (?s)max_by.*does not support ordering on type

    @sail-bug
    Scenario: max_by rejects an ARRAY<MAP> ordering column
      When query
        """
        SELECT max_by(x, array(map('k', y))) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error (?s)max_by.*does not support ordering on type

    # The check is recursive, so a MAP or VARIANT buried at any depth also fails.
    @sail-bug
    Scenario Outline: max_by rejects a nested <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(x, y)
        """
      Then query error (?s)max_by.*does not support ordering on type

      Examples:
        | case               | ordering                   |
        | STRUCT<MAP>        | struct(map('k', y))        |
        | ARRAY<ARRAY<MAP>>  | array(array(map('k', y)))  |
        | STRUCT<ARRAY<MAP>> | struct(array(map('k', y))) |

    # parse_json is Spark 4.0+, so the VARIANT rows are split out of the outline above to keep
    # the MAP coverage available when the suite runs against the 3.5 oracle.
    @sail-bug
    @spark-4
    Scenario Outline: max_by rejects a nested Spark 4 <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(x, y)
        """
      Then query error (?s)max_by.*does not support ordering on type

      Examples:
        | case            | ordering              |
        | ARRAY<VARIANT>  | array(parse_json(x))  |
        | STRUCT<VARIANT> | struct(parse_json(x)) |

    # Spark's CalendarIntervalType is not an AtomicType, so it is not orderable,
    # unlike the ANSI day-time and year-month interval types below.
    @sail-bug
    Scenario: max_by rejects a calendar INTERVAL ordering column
      When query
        """
        SELECT max_by(x, make_interval(0, 0, 0, y)) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error (?s)max_by.*does not support ordering on type

    # The rejections above only discriminate if the accepted types are pinned too: a check that
    # rejected every type would satisfy every scenario in this Rule. These are the orderable
    # counterparts, including the ANSI intervals the calendar one above is contrasted with.
    # The winning row is deliberately in the MIDDLE of the input: Sail rewrites max_by to
    # `last_value(x) ORDER BY y`, so a fixture whose maximum is also its last row would stay
    # green even if the ordering key were dropped entirely.
    Scenario Outline: max_by accepts an orderable <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('lo', 1), ('hi', 3), ('mid', 2) AS t(x, y)
        """
      Then query result
        | result |
        | hi     |

      Examples:
        | case                   | ordering                                                       |
        | STRUCT<INT>            | struct(y)                                                      |
        | STRUCT<STRUCT>         | struct(struct(y))                                              |
        | unmarked VARIANT shape | named_struct('metadata', CAST(CAST(y AS STRING) AS BINARY), 'value', CAST(CAST(y AS STRING) AS BINARY)) |
        | ARRAY<INT>             | array(y)                                                       |
        | ARRAY<STRUCT>          | array(struct(y))                                               |
        | STRUCT<ARRAY<INT>>     | struct(array(y))                                               |
        | INTERVAL DAY TO SECOND | make_dt_interval(0, 0, 0, y)                                   |
        | INTERVAL YEAR          | make_ym_interval(0, y)                                         |
        | BINARY                 | CAST(CAST(y AS STRING) AS BINARY)                              |
        | DECIMAL                | CAST(y AS DECIMAL(10,2))                                       |
        | DATE                   | date_add(DATE '2024-01-01', y)                                 |
        | TIMESTAMP              | TIMESTAMP '2024-01-01 00:00:00' + make_dt_interval(0, 0, 0, y) |

    # VOID is orderable in Spark: OrderUtils.isOrderable matches `case NullType => true` before
    # the AtomicType case. So the call is accepted, and every row is then skipped for having a
    # NULL ordering value, which is what makes the result NULL rather than an error.
    @sail-bug
    Scenario: max_by accepts a VOID ordering column and returns NULL
      When query
        """
        SELECT max_by(x, CAST(NULL AS VOID)) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |

    # OrderUtils.isOrderable rejects GEOMETRY and GEOGRAPHY explicitly, before the AtomicType
    # case, because they are opaque WKB bytes with no meaningful ordering. Sail lowers both to
    # plain BINARY and keeps the geo identity in the field metadata, so `coerce_types` cannot
    # see it; the check is repeated in `return_field`, which does receive the fields.
    @sail-bug
    @spark-4.2
    Scenario: max_by rejects a GEOMETRY ordering column
      When query
        """
        SELECT max_by(v, st_geomfromwkb(w)) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)max_by.*does not support ordering on type

    # Spark recurses into ARRAY and STRUCT, so a nested GEOMETRY is unorderable too. The check
    # only sees it if `array()` and `named_struct()` keep the child's geo metadata.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by rejects a GEOMETRY nested by <case>
      When query
        """
        SELECT max_by(v, <ordering>) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)max_by.*does not support ordering on type

      Examples:
        | case         | ordering                             |
        | array        | array(st_geomfromwkb(w))             |
        | named_struct | named_struct('g', st_geomfromwkb(w)) |

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
    Scenario: max_by rejects a MAP ordering column in a window frame
      When query
        """
        SELECT max_by(v, o) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('lo', map('a', 1), 1), ('hi', map('b', 2), 2) AS t(v, o, i)
        """
      Then query error does not support ordering on type

  Rule: Clause surface

    # These are not orderability rules: Spark decides them in FunctionResolution.validateFunction
    # and in CheckAnalysis, for the function itself and in either position (aggregate or window).

    Scenario Outline: max_by accepts the clause <case>
      When query
        """
        SELECT <expr> AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | expr                               | result |
        | plain    | max_by(x, y)                       | b      |
        | DISTINCT | max_by(DISTINCT x, y)              | b      |
        | FILTER   | max_by(x, y) FILTER (WHERE y < 50) | c      |

    # A window aggregate emits one row per input row, so this returns four.
    Scenario: max_by accepts the clause OVER
      When query
        """
        SELECT max_by(x, y) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query result
        | result |
        | b      |
        | b      |
        | b      |
        | b      |

    # applyIgnoreNulls has a whitelist (NthValue, Lead, Lag, First, Last, AnyValue,
    # CollectList, CollectSet); max_by is not in it. Honoring the clause would answer 'b' where
    # the plain call answers NULL.
    @sail-bug
    @spark-4
    Scenario: max_by rejects the clause IGNORE NULLS
      When query
        """
        SELECT max_by(x, y) IGNORE NULLS AS result
        FROM VALUES (CAST(NULL AS STRING), 50), ('b', 10) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support IGNORE NULLS

    # WITHIN GROUP requires SupportsOrderingWithinGroup, which max_by is not. Keeping the
    # user's ORDER BY ahead of the ordering key would answer 'c' instead of 'b'.
    @sail-bug
    @spark-4
    Scenario: max_by rejects the clause WITHIN GROUP
      When query
        """
        SELECT max_by(x, y) WITHIN GROUP (ORDER BY x) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support WITHIN GROUP

    # `WindowResolution.checkWindowFunction` rejects any DISTINCT aggregate used as a window function.
    @sail-bug
    Scenario: max_by rejects the clause DISTINCT combined with OVER
      When query
        """
        SELECT max_by(DISTINCT x, y) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query error DISTINCT_WINDOW_FUNCTION_UNSUPPORTED

    # The mirror image: Spark 4.2 allows FILTER on a window aggregate.
    @sail-bug
    @spark-4.2
    Scenario: max_by supports the clause FILTER combined with OVER
      When query
        """
        SELECT max_by(x, y) FILTER (WHERE y < 50) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query result
        | result |
        | c      |
        | c      |
        | c      |
        | c      |

    # Spark names an unaliased aggregate with its FILTER clause, as `<call> FILTER (WHERE <condition>)`.
    @sail-bug
    Scenario: max_by names an unaliased result with its FILTER clause
      When query
        """
        SELECT max_by(x, y) FILTER (WHERE y < 50)
        FROM VALUES ('a', 10), ('b', 50) AS t(x, y)
        """
      Then query schema
        """
        root
         |-- max_by(x, y) FILTER (WHERE (y < 50)): string (nullable = true)
        """

    # Sail drops both halves of the window name: the `FILTER` clause, because
    # `filter_max_min_by_window_arguments` moves the predicate into a `CASE WHEN` and builds the
    # window function with no filter of its own, and the `OVER (...)` suffix, which Sail omits for
    # every window function.
    @sail-bug @spark-4.2
    Scenario: max_by names an unaliased window result with its FILTER clause
      When query
        """
        SELECT max_by(x, y) FILTER (WHERE y > 15) OVER ()
        FROM VALUES (1, 10), (2, 30) AS t(x, y)
        """
      Then query schema
        """
        root
         |-- max_by(x, y) FILTER (WHERE (y > 15)) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING): integer (nullable = true)
        """

  Rule: Arity

    # Spark's MaxMinBy is BinaryLike. Sail used to panic here and kill the RPC, because
    # Signature::user_defined skips DataFusion's arity gate and coerce_types is the only one.
    @sail-bug
    Scenario Outline: max_by rejects a call with <case>
      When query
        """
        SELECT max_by(<args>)
        """
      Then query error (?si)max_by.*requires

      Examples:
        | case            | args       |
        | no arguments    |            |
        | one argument    | 1          |
        | four arguments  | 1, 2, 3, 4 |

    # Spark 4.2 added the top-k form, which returns an array of the k values
    # (MaxMinByK.scala).
    @sail-bug
    @spark-4.2
    Scenario: max_by supports the three-argument top-k form
      When query
        """
        SELECT max_by(x, y, 2) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result   |
        | [b, c]   |

  Rule: Constant and untyped ordering keys

    # A constant ordering key makes every row tie, and Spark documents the winner among tied
    # rows as unspecified (`MaxByAndMinBy.scala`: "the output can be different for those
    # associated the same values"), because partial-state merge order is not guaranteed. So
    # assert only that the call is accepted and returns one of the tied values; the running
    # window scenarios above pin the directional tie rule where it IS deterministic.
    # Foldable arguments are constant-folded to the same literal.
    Scenario Outline: max_by accepts the constant ordering key <case>
      When query
        """
        SELECT max_by(x, <ordering>) IN ('a', 'b', 'c') AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case            | ordering         |
        | literal         | 1                |
        | foldable sum    | 1 + 1            |
        | foldable concat | concat('a', 'b') |

    # A unique window ORDER BY makes tie replacement deterministic inside every running frame:
    # Spark's predicate is strict, so each row's tie with the buffered key takes the newer row.
    @sail-bug
    Scenario: max_by takes the newer row on ties in a running window
      When query
        """
        SELECT i,
               max_by(x, 1) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(i, x)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | b      |
        | 3 | c      |

    # NullType is orderable in Spark, which returns NULL here.
    @sail-bug
    Scenario: max_by accepts an untyped NULL ordering argument
      When query
        """
        SELECT max_by(x, NULL) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |

  Rule: The value argument has no orderability restriction

    # Only the ordering argument is checked, so a MAP or VARIANT value is fine.
    # Winning row in the middle again, for the reason given on the orderable table above.
    Scenario Outline: max_by accepts a <case> value argument
      When query
        """
        SELECT <value> AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":3}', 3), ('{"v":2}', 2) AS t(j, y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | value                                      | result  |
        | MAP         | max_by(map('a', y), y)['a']                | 3       |
        | ARRAY<MAP>  | max_by(array(map('k', y)), y)[0]['k']      | 3       |
        | STRUCT<MAP> | max_by(struct(map('k', y) AS m), y).m['k'] | 3       |

    @spark-4
    Scenario: max_by accepts a VARIANT value argument
      When query
        """
        SELECT to_json(max_by(parse_json(j), y)) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":3}', 3), ('{"v":2}', 2) AS t(j, y)
        """
      Then query result
        | result  |
        | {"v":3} |

  Rule: Spark ordering semantics of the ordering argument

    # `MaxByAndMinBy.scala` updates with `If(old > new, old, new)`, so equal keys take the newer
    # row. The comparison is SQL ordering, where -0.0 equals 0.0, so a running window must switch
    # to the newer row. An IEEE total order would rank -0.0 below 0.0 and keep the older row.
    @sail-bug
    Scenario: max_by treats negative zero and zero DOUBLE ordering keys as equal in a running window
      When query
        """
        SELECT i,
               max_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'pos', 0.0D), (2, 'neg', -0.0D) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | pos    |
        | 2 | neg    |

    # In Spark's nested ordering a NULL array element is smaller than a non-null one, so the
    # newer `array(NULL)` never wins over `array(1)`. DataFusion's `ScalarValue::partial_cmp`
    # follows Postgres instead and ranks the NULL element greater.
    @sail-bug
    Scenario: max_by ranks a NULL array element below a non-null one in a running window
      When query
        """
        SELECT i,
               max_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'one', array(1)), (2, 'null', array(CAST(NULL AS INT))) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | one    |
        | 2 | one    |

    # The same rule decides the aggregate form: ascending with NULLS FIRST ranks the nested NULL
    # below the non-null value, so the non-null row wins.
    Scenario: max_by ranks a NULL struct field below a non-null one in an aggregate
      When query
        """
        SELECT max_by(x, y) AS result
        FROM VALUES ('one', named_struct('a', 1)), ('null', named_struct('a', CAST(NULL AS INT))) AS t(x, y)
        """
      Then query result
        | result |
        | one    |

    # A NULL struct field is likewise smaller than a non-null one in Spark, whichever row is newer.
    # DataFusion's `partial_cmp_struct` skips the NULL position and answers `Equal`, which would
    # turn the comparison into a tie, so both row orders are pinned.
    Scenario Outline: max_by ranks a NULL struct field below a non-null one in a running window with <case>
      When query
        """
        SELECT i,
               max_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, '<first>', named_struct('a', <first_key>)), (2, '<second>', named_struct('a', <second_key>)) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result   |
        | 1 | <first>  |
        | 2 | <result> |

      Examples:
        | case                 | first | first_key         | second | second_key        | result |
        | the NULL field newer | one   | 1                 | null   | CAST(NULL AS INT) | one    |

    # A NULL struct field is likewise smaller than a non-null one in Spark, whichever row is newer.
    # DataFusion's `partial_cmp_struct` skips the NULL position and answers `Equal`, which would
    # turn the comparison into a tie, so both row orders are pinned.
    @sail-bug
    Scenario Outline: max_by ranks a NULL struct field below a non-null one in a running window with <case> (known Sail bug)
      When query
        """
        SELECT i,
               max_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, '<first>', named_struct('a', <first_key>)), (2, '<second>', named_struct('a', <second_key>)) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result   |
        | 1 | <first>  |
        | 2 | <result> |

      Examples:
        | case                 | first | first_key         | second | second_key        | result |
        | the NULL field older | null  | CAST(NULL AS INT) | one    | 1                 | one    |

  Rule: The value argument keeps its logical type

    # `dataType = valueExpr.dataType`, so the result is still a GEOMETRY. Sail carries GEOMETRY as
    # BINARY plus field metadata, so the result field must be the value field, not one rebuilt
    # from its `DataType`.
    @sail-bug
    @spark-4.2
    Scenario: max_by keeps the GEOMETRY type of the value argument
      When query
        """
        SELECT max_by(st_geomfromwkb(w), y) AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040') AS t(y, w)
        """
      Then query schema
        """
        root
         |-- result: geometry (nullable = true)
        """

  Rule: Spark ordering boundaries that Sail already matches

    # These keys all have a strict winner, so both the aggregate and the whole-partition window
    # form are deterministic. Each boundary is one Sail could get wrong: NaN is the largest double
    # in Spark's SQL ordering, BINARY compares unsigned bytes, a pre-epoch fraction must not flip
    # sign, DECIMAL(38,0) sits at the 128-bit limit, and a longer array wins over its own prefix.
    Scenario Outline: max_by follows Spark ordering for <case> ordering keys in the <path> form
      When query
        """
        SELECT DISTINCT <call> AS result
        FROM VALUES <rows> AS t(x, o)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | path      | call                 | rows                                                                                                                       | result |
        | NaN above infinity             | aggregate | max_by(x, o)         | ('one', 1.0D), ('inf', CAST('Infinity' AS DOUBLE)), ('nan', CAST('NaN' AS DOUBLE)), ('ninf', CAST('-Infinity' AS DOUBLE))  | nan    |
        | NaN above infinity             | window    | max_by(x, o) OVER () | ('one', 1.0D), ('inf', CAST('Infinity' AS DOUBLE)), ('nan', CAST('NaN' AS DOUBLE)), ('ninf', CAST('-Infinity' AS DOUBLE))  | nan    |
        | unsigned BINARY                | aggregate | max_by(x, o)         | ('lo', X'01'), ('hi', X'FF'), ('mid', X'7F')                                                                               | hi     |
        | unsigned BINARY                | window    | max_by(x, o) OVER () | ('lo', X'01'), ('hi', X'FF'), ('mid', X'7F')                                                                               | hi     |
        | pre-epoch fractional TIMESTAMP | aggregate | max_by(x, o)         | ('a', TIMESTAMP '1969-12-31 23:59:59.5'), ('b', TIMESTAMP '1969-12-31 23:59:59.9'), ('c', TIMESTAMP '1969-12-31 23:59:59') | b      |
        | pre-epoch fractional TIMESTAMP | window    | max_by(x, o) OVER () | ('a', TIMESTAMP '1969-12-31 23:59:59.5'), ('b', TIMESTAMP '1969-12-31 23:59:59.9'), ('c', TIMESTAMP '1969-12-31 23:59:59') | b      |
        | DECIMAL(38,0) extremes         | aggregate | max_by(x, o)         | ('max', 99999999999999999999999999999999999999BD), ('min', -99999999999999999999999999999999999999BD), ('zero', 0BD)       | max    |
        | DECIMAL(38,0) extremes         | window    | max_by(x, o) OVER () | ('max', 99999999999999999999999999999999999999BD), ('min', -99999999999999999999999999999999999999BD), ('zero', 0BD)       | max    |
        | ARRAY prefix length            | aggregate | max_by(x, o)         | ('short', array(1, 2)), ('long', array(1, 2, 0)), ('big', array(0, 9))                                                     | long   |
        | ARRAY prefix length            | window    | max_by(x, o) OVER () | ('short', array(1, 2)), ('long', array(1, 2, 0)), ('big', array(0, 9))                                                     | long   |
        | NULL array element             | aggregate | max_by(x, o)         | ('one', array(1)), ('null', array(CAST(NULL AS INT))), ('zero', array(0))                                                  | one    |
        | NULL struct field              | aggregate | max_by(x, o)         | ('one', named_struct('a', 1)), ('null', named_struct('a', CAST(NULL AS INT))), ('zero', named_struct('a', 0))              | one    |

    # NaN == NaN in Spark's SQL ordering, so every row ties and the strict predicate takes the
    # newer one. `ScalarValue::partial_cmp` agrees here because `total_cmp` also equates NaNs.
    @sail-bug
    Scenario: max_by takes the newer row on NaN ties in a running window
      When query
        """
        SELECT i,
               max_by(x, o) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a', CAST('NaN' AS DOUBLE)), (2, 'b', CAST('NaN' AS DOUBLE)) AS t(i, x, o)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | b      |

  Rule: Spark ordering semantics that Sail does not match yet

    # -0.0 equals 0.0 in Spark's SQL ordering, so the first fields tie and the second field
    # decides. No tie between rows is involved, so both forms are deterministic. An IEEE total
    # order would rank -0.0 below 0.0 before the second field is ever read.
    @sail-bug
    Scenario Outline: max_by treats negative zero and zero as equal inside a STRUCT ordering key in the <path> form
      When query
        """
        SELECT DISTINCT <call> AS result
        FROM VALUES ('pos', named_struct('a', 0.0D, 'b', 1)), ('neg', named_struct('a', -0.0D, 'b', 2)) AS t(x, o)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path      | call                 | result |
        | aggregate | max_by(x, o)         | neg    |
        | window    | max_by(x, o) OVER () | neg    |

    # A NULL ordering key is skipped, and a VOID key is NULL on every row, so Spark returns NULL.
    # A `NullArray` has no validity buffer, so the window accumulator must read its logical nulls.
    @sail-bug
    Scenario: max_by skips a VOID ordering key in a running window
      When query
        """
        SELECT i,
               max_by(x, CAST(NULL AS VOID)) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a'), (2, 'b') AS t(i, x)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | NULL   |

    # The aggregate counterpart of the array window scenario above: ascending with NULLS FIRST
    # ranks the NULL element below the non-null ones, so it never wins.
    Scenario: max_by ranks a NULL array element below a non-null one in an aggregate
      When query
        """
        SELECT max_by(x, o) AS result
        FROM VALUES ('one', array(1)), ('null', array(CAST(NULL AS INT))), ('zero', array(0)) AS t(x, o)
        """
      Then query result
        | result |
        | one    |

    # The window form reaches the same `return_field`, so it keeps the GEOMETRY metadata too.
    @sail-bug
    @spark-4.2
    Scenario: max_by keeps the GEOMETRY type of the value argument as a window function
      When query
        """
        SELECT max_by(st_geomfromwkb(w), y) OVER () AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040') AS t(y, w)
        """
      Then query schema
        """
        root
         |-- result: geometry (nullable = true)
        """

  Rule: Aggregate surface

    # Sail rewrites the aggregate form to an ordered `last_value` with an added
    # `ordering IS NOT NULL` filter, so these pin the shapes that rewrite has to survive. A NULL
    # value is not skipped: only a NULL ordering key is, so a NULL at the extremum is the answer.
    # The code-point case holds because Spark compares UTF-8 bytes, not UTF-16 code units.
    Scenario Outline: max_by returns the expected value for <case>
      When query
        """
        <query>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | query                                                                                                                                                                | result |
        | empty input                                      | SELECT max_by(x, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y) WHERE false                                                                     | NULL   |
        | single row                                       | SELECT max_by(x, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y) WHERE x = 'c'                                                                   | c      |
        | a NULL value at the extremum                     | SELECT max_by(CASE WHEN y = 50 THEN NULL ELSE x END, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                             | NULL   |
        | a negated ordering expression                    | SELECT max_by(x, -y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                                | a      |
        | a CASE ordering expression                       | SELECT max_by(x, CASE WHEN x = 'c' THEN 100 ELSE y END) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                             | c      |
        | the value and ordering being the same column     | SELECT max_by(y, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                                 | 50     |
        | a constant value                                 | SELECT max_by('k', y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                               | k      |
        | an ordering column from a joined table           | SELECT max_by(a.x, b.w) AS result FROM VALUES ('a', 1), ('b', 2), ('c', 3) AS a(x, id) JOIN VALUES (1, 30), (2, 10), (3, 20) AS b(id, w) ON a.id = b.id              | a      |
        | a CTE                                            | WITH s AS (SELECT * FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)) SELECT max_by(x, y) AS result FROM s                                                     | b      |
        | try_cast NULLs in the ordering                   | SELECT max_by(x, try_cast(o AS INT)) AS result FROM VALUES ('a', '1'), ('b', 'zz'), ('c', '3') AS t(x, o)                                                            | c      |
        | long strings sharing a prefix                    | SELECT max_by(x, o) AS result FROM VALUES ('short', repeat('a', 5000)), ('long', concat(repeat('a', 5000), 'b')), ('mid', concat(repeat('a', 4999), 'b')) AS t(x, o) | mid    |
        | a supplementary character against a high BMP one | SELECT max_by(x, o) AS result FROM VALUES ('emoji', '😀'), ('bmp', '｡') AS t(x, o)                                                                                    | emoji  |

    # The FILTER predicate is ANDed with the rewrite's own NULL filter, per group.
    Scenario: max_by combines FILTER with a GROUP BY that has a NULL group key
      When query
        """
        SELECT g, max_by(x, y) FILTER (WHERE i > 1) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        GROUP BY g
        ORDER BY g NULLS FIRST
        """
      Then query result ordered
        | g    | result |
        | NULL | d      |
        | g1   | b      |
        | g2   | e      |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k combines FILTER with a GROUP BY that has a NULL group key
      When query
        """
        SELECT g, max_by(x, y, 2) FILTER (WHERE i > 1) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        GROUP BY g
        ORDER BY g NULLS FIRST
        """
      Then query result ordered
        | g    | result |
        | NULL | [d, c] |
        | g1   | [b]    |
        | g2   | [e]    |

    Scenario: max_by works with GROUPING SETS
      When query
        """
        SELECT g, max_by(x, y) AS result, grouping(g) AS gg
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        GROUP BY GROUPING SETS ((g), ())
        ORDER BY gg, g NULLS FIRST
        """
      Then query result ordered
        | g    | result | gg |
        | NULL | d      | 0  |
        | g1   | b      | 0  |
        | g2   | e      | 0  |
        | NULL | b      | 1  |

  Rule: Window surface

    # A NULL partition key forms its own partition, and a named WINDOW resolves like an inline one.
    Scenario: max_by over a named window partitioned by a key with NULLs
      When query
        """
        SELECT i, max_by(x, y) OVER w AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        WINDOW w AS (PARTITION BY g)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | b      |
        | 2 | b      |
        | 3 | d      |
        | 4 | d      |
        | 5 | e      |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k over a named window partitioned by a key with NULLs
      When query
        """
        SELECT i, max_by(x, y, 2) OVER w AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        WINDOW w AS (PARTITION BY g)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | [b, a] |
        | 2 | [b, a] |
        | 3 | [d, c] |
        | 4 | [d, c] |
        | 5 | [e]    |

    # With ORDER BY and no frame, the default frame is RANGE up to the current row, so ORDER BY
    # peers (here the NULL keys, sorted first) are all inside each other's frame.
    Scenario: max_by includes ORDER BY peers in the default window frame
      When query
        """
        SELECT i, max_by(x, y) OVER (ORDER BY g) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | b      |
        | 2 | b      |
        | 3 | d      |
        | 4 | d      |
        | 5 | b      |

    # Spark's update expression keeps NULL while no row has had a non-null key yet, and then
    # ignores rows whose key is NULL.
    Scenario: max_by skips NULL ordering keys in a running window
      When query
        """
        SELECT i,
               max_by(x, CASE WHEN i IN (1, 3) THEN NULL ELSE y END) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | b      |
        | 3 | b      |
        | 4 | b      |
        | 5 | b      |

    # Any frame that does not start at UNBOUNDED PRECEDING needs a retractable accumulator: at
    # row 4 the buffered maximum leaves the frame and the next best row has to take over.
    @sail-bug
    Scenario: max_by supports a sliding window frame
      When query
        """
        SELECT i, max_by(x, y) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | b      |
        | 3 | b      |
        | 4 | d      |
        | 5 | d      |

    # A frame that reaches FOLLOWING rows retracts from its front while rows enter at its back.
    # NULL keys enter and leave it without ever winning, and at row 2 `a` and `c` tie on 3, so the newer `c` wins.
    @sail-bug
    Scenario: max_by retracts ties and NULL keys in a sliding window frame
      When query
        """
        SELECT i, max_by(x, y) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS result
        FROM VALUES (1, 'a', 3), (2, 'b', 1), (3, 'c', 3), (4, 'd', CAST(NULL AS INT)), (5, 'e', 2), (6, 'f', 2), (7, 'g', 5), (8, 'h', CAST(NULL AS INT)) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | c      |
        | 3 | c      |
        | 4 | c      |
        | 5 | c      |
        | 6 | g      |
        | 7 | g      |
        | 8 | g      |

  Rule: Named arguments

    # `MaxByBuilder` is a plain ExpressionBuilder with no `functionSignature`, so Spark
    # rejects named arguments in analysis.
    @sail-bug
    Scenario: max_by rejects named arguments
      When query
        """
        SELECT max_by(x => x, y => y) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error NAMED_PARAMETERS_NOT_SUPPORTED

    # The named-argument check applies to known built-ins only: Spark resolves the routine first,
    # so an unknown one is reported as UNRESOLVED_ROUTINE, never as named-argument misuse.
    Scenario: an unknown function called with named arguments is not reported as named-argument misuse
      When query
        """
        SELECT no_such_function_zzz(a => 1) AS result
        """
      Then query error (?s)\A(?!.*NAMED_PARAMETERS_NOT_SUPPORTED).*no_such_function_zzz

  Rule: Output schema

    # `dataType = valueExpr.dataType` and `nullable = true`, whatever the value's own nullability.
    Scenario Outline: max_by returns a nullable result for a <case> value argument
      When query
        """
        SELECT max_by(<value>, y) AS result
        FROM VALUES ('a', 10, 1), ('b', 50, 2), ('c', 20, 3) AS t(x, y, i)
        """
      Then query schema
        """
        root
         |-- <schema>
        """

      Examples:
        | case          | value                               | schema                                  |
        | INT           | i                                   | result: integer (nullable = true)       |
        | DECIMAL       | CAST(i AS DECIMAL(5, 2))            | result: decimal(5,2) (nullable = true)  |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-01 00:00:00' | result: timestamp_ntz (nullable = true) |

    # PySpark renders `NullType` as `void` in the schema tree from 4.0 and as `null` before.
    @spark-4
    Scenario: max_by returns a nullable result for a VOID value argument
      When query
        """
        SELECT max_by(NULL, y) AS result
        FROM VALUES ('a', 10, 1), ('b', 50, 2), ('c', 20, 3) AS t(x, y, i)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    # The nested nullability flags come from the value argument unchanged.
    Scenario: max_by keeps the nested nullability of an ARRAY value argument
      When query
        """
        SELECT max_by(array(i, NULL), y) OVER () AS result
        FROM VALUES ('a', 10, 1), ('b', 50, 2), ('c', 20, 3) AS t(x, y, i)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """

  Rule: The top-k form

    # `MaxMinByK` returns the values of the k rows with the largest orderings, sorted descending
    # by the ordering, as `ARRAY<value type>`. It skips NULL orderings but keeps NULL values,
    # returns NULL when no ordering is non-null, and casts k to INT. These inputs have no ties
    # inside the first k rows, so the order of each array is deterministic.
    @spark-4.2
    Scenario Outline: max_by returns the top-k values for <case>
      When query
        """
        SELECT <call> AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g) <where>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | call                                    | where       | result             |
        | no input rows                       | max_by(x, y, 2)                         | WHERE false | NULL               |

    # `MaxMinByK` returns the values of the k rows with the largest orderings, sorted descending
    # by the ordering, as `ARRAY<value type>`. It skips NULL orderings but keeps NULL values,
    # returns NULL when no ordering is non-null, and casts k to INT. These inputs have no ties
    # inside the first k rows, so the order of each array is deterministic.
    @spark-4.2
    @sail-bug
    Scenario Outline: max_by returns the top-k values for <case> (known Sail bug)
      When query
        """
        SELECT <call> AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g) <where>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | call                                    | where       | result             |
        | k of 2                              | max_by(x, y, 2)                         |             | [b, NULL]          |
        | k above the row count               | max_by(x, y, 10)                        |             | [b, NULL, f, c, a] |
        | k of 1                              | max_by(x, y, 1)                         |             | [b]                |
        | k as a string                       | max_by(x, y, '2')                       |             | [b, NULL]          |
        | k as a double                       | max_by(x, y, 2.0)                       |             | [b, NULL]          |
        | k as a foldable expression          | max_by(x, y, 1 + 1)                     |             | [b, NULL]          |
        | k at the maximum                    | max_by(x, y, 100000)                    |             | [b, NULL, f, c, a] |
        | only NULL orderings                 | max_by(x, CAST(NULL AS INT), 2)         |             | NULL               |
        | FILTER                              | max_by(x, y, 2) FILTER (WHERE g = 'g2') |             | [f, c]             |
        | a STRUCT ordering with a NULL field | max_by(x, named_struct('a', y), 3)      |             | [b, NULL, f]       |

    # DISTINCT removes duplicate (value, ordering) pairs before the k rows are chosen.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k removes duplicate pairs with DISTINCT
      When query
        """
        SELECT max_by(DISTINCT x, y, 2) AS result
        FROM VALUES ('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 0) AS t(x, y)
        """
      Then query result
        | result |
        | [b, a] |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k is computed per group
      When query
        """
        SELECT g, max_by(x, y, 2) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g  | result    |
        | g1 | [b, NULL] |
        | g2 | [f, c]    |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k in a running window
      When query
        """
        SELECT i, max_by(x, y, 2) OVER (ORDER BY i) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        ORDER BY i
        """
      Then query result ordered
        | i | result    |
        | 1 | [a]       |
        | 2 | [b, a]    |
        | 3 | [b, c]    |
        | 4 | [b, c]    |
        | 5 | [b, NULL] |
        | 6 | [b, NULL] |

    # A sliding frame has to drop rows as they leave, including the top values themselves.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k in a sliding window frame
      When query
        """
        SELECT i, max_by(x, y, 2) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        ORDER BY i
        """
      Then query result ordered
        | i | result    |
        | 1 | [a]       |
        | 2 | [b, a]    |
        | 3 | [b, c]    |
        | 4 | [b, c]    |
        | 5 | [NULL, c] |
        | 6 | [NULL, f] |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k returns an array of nullable values
      When query
        """
        SELECT max_by(x, y, 2) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    # `MaxMinByK.checkInputDataTypes`: k must be a foldable INT within [1, 100000]; a NULL k reads
    # as 0, and a type Spark cannot implicitly cast to INT is rejected before the range check.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by top-k rejects <case>
      When query
        """
        SELECT max_by(x, y, <k>) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        """
      Then query error <error>

      Examples:
        | case                  | k                 | error                                                        |
        | a NULL k              | NULL              | The .k. must be between .1, 100000. .current value = 0.      |
        | a zero k              | 0                 | The .k. must be between .1, 100000. .current value = 0.      |
        | a negative k          | -1                | The .k. must be between .1, 100000. .current value = -1.     |
        | a k above the maximum | 100001            | The .k. must be between .1, 100000. .current value = 100001. |
        | a non-foldable k      | i                 | the input k should be a foldable int expression              |
        | a DATE k              | DATE '2024-01-01' | third parameter requires the "INT" type                      |

    # Spark checks k in analysis, so the error does not depend on any group surviving.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k rejects an out-of-range k when no group survives
      When query
        """
        SELECT g, max_by(x, y, 0) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        WHERE false
        GROUP BY g
        """
      Then query error The .k. must be between .1, 100000. .current value = 0.

  Rule: Rejections carry Spark's error class

    # Spark reports these in analysis with an error class; clients match on the class rather than
    # on the free text, so the class name is asserted alongside the message core.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by reports the Spark error class for <case>
      When query
        """
        SELECT <call> AS result
        FROM VALUES (1, 'a', 10), (2, 'b', 50) AS t(i, x, y)
        """
      Then query error <error>

      Examples:
        | case                  | call                            | error                                                                          |
        | an unorderable key    | max_by(x, map('k', y))          | (?s)DATATYPE_MISMATCH.INVALID_ORDERING_TYPE.*does not support ordering on type |
        | an out-of-range k     | max_by(x, y, 0)                 | (?s)DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE.*The .k. must be between              |
        | a non-foldable k      | max_by(x, y, i)                 | (?s)DATATYPE_MISMATCH.NON_FOLDABLE_INPUT.*foldable int expression              |
        | a k of the wrong type | max_by(x, y, DATE '2024-01-01') | (?s)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE.*requires the "INT" type           |
        | four arguments        | max_by(x, y, 2, 3)              | (?s)WRONG_NUM_ARGS.*requires .2, 3. parameters                                 |

    # A GEOMETRY key is unorderable however it is produced. DataFusion builds the field of a
    # `CASE` and of the null-selecting functions from the branch `DataType` alone
    # (`expr_schema.rs`), which drops the geo metadata, so the branches are judged one by one.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by rejects a GEOMETRY ordering key produced by <case>
      When query
        """
        SELECT max_by(v, <key>) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)max_by.*does not support ordering on type

      Examples:
        | case     | key                                                   |
        | CASE     | CASE WHEN true THEN st_geomfromwkb(w) END             |
        | IF       | IF(true, st_geomfromwkb(w), NULL)                     |
        | COALESCE | COALESCE(st_geomfromwkb(w), st_geomfromwkb(w))        |
        | NVL      | nvl(st_geomfromwkb(w), st_geomfromwkb(w))             |
        | NVL2     | nvl2(w, st_geomfromwkb(w), st_geomfromwkb(w))         |

    # `MaxMinByK.checkInputDataTypes` runs its checks in a fixed order: the implicit cast of `k`
    # to INT first, then the foldability of `k`, then the ordering type, and the range of `k`
    # last, so a call that fails two of them reports the earlier one.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by reports the earlier check when the top-k call fails <case>
      When query
        """
        SELECT max_by(x, map('k', y), <k>) AS result
        FROM VALUES (1, 'a', 10), (2, 'b', 50) AS t(i, x, y)
        """
      Then query error <error>

      Examples:
        | case                                 | k                 | error                                   |
        | the ordering type and the range of k | 0                 | DATATYPE_MISMATCH.INVALID_ORDERING_TYPE |
        | the ordering type and a foldable k   | i                 | DATATYPE_MISMATCH.NON_FOLDABLE_INPUT    |
        | the ordering type and the type of k  | DATE '2024-01-01' | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE |

  Rule: Grouping extensions

    Scenario: max_by is computed per ROLLUP grouping set
      When query
        """
        SELECT k, p, max_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY ROLLUP(k, p)
        ORDER BY k NULLS FIRST, p NULLS FIRST
        """
      Then query result ordered
        | k    | p    | r |
        | NULL | NULL | b |
        | k1   | NULL | b |
        | k1   | x    | a |
        | k1   | y    | b |
        | k2   | NULL | d |
        | k2   | x    | d |

    Scenario: max_by is computed per CUBE grouping set
      When query
        """
        SELECT k, p, max_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY CUBE(k, p)
        ORDER BY k NULLS FIRST, p NULLS FIRST
        """
      Then query result ordered
        | k    | p    | r |
        | NULL | NULL | b |
        | NULL | x    | d |
        | NULL | y    | b |
        | k1   | NULL | b |
        | k1   | x    | a |
        | k1   | y    | b |
        | k2   | NULL | d |
        | k2   | x    | d |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k is computed per ROLLUP grouping set
      When query
        """
        SELECT k, max_by(v, o, 2) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY ROLLUP(k)
        ORDER BY k NULLS FIRST
        """
      Then query result ordered
        | k    | r      |
        | NULL | [b, d] |
        | k1   | [b, a] |
        | k2   | [d, c] |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k is computed per CUBE grouping set
      When query
        """
        SELECT k, max_by(v, o, 2) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY CUBE(k)
        ORDER BY k NULLS FIRST
        """
      Then query result ordered
        | k    | r      |
        | NULL | [b, d] |
        | k1   | [b, a] |
        | k2   | [d, c] |

    Scenario: max_by can be used in HAVING
      When query
        """
        SELECT k, max_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY k
        HAVING max_by(v, o) <> 'a'
        ORDER BY k
        """
      Then query result ordered
        | k  | r |
        | k1 | b |
        | k2 | d |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k can be used in HAVING
      When query
        """
        SELECT k, max_by(v, o, 2) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY k
        HAVING max_by(v, o, 1)[0] <> 'a'
        ORDER BY k
        """
      Then query result ordered
        | k  | r      |
        | k1 | [b, a] |
        | k2 | [d, c] |

    Scenario: max_by in each branch of a UNION ALL
      When query
        """
        SELECT max_by(v, o) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k1'
        UNION ALL
        SELECT max_by(v, o) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k2'
        ORDER BY r
        """
      Then query result ordered
        | r |
        | b |
        | d |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k in each branch of a UNION ALL
      When query
        """
        SELECT max_by(v, o, 2) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k1'
        UNION ALL
        SELECT max_by(v, o, 2) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k2'
        ORDER BY r
        """
      Then query result ordered
        | r      |
        | [b, a] |
        | [d, c] |

    @sail-bug
    Scenario: max_by in a RANGE window frame
      When query
        """
        SELECT o, max_by(v, o) OVER (ORDER BY o RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        ORDER BY o
        """
      Then query result ordered
        | o  | r |
        | 10 | a |
        | 20 | c |
        | 30 | d |
        | 50 | b |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k in a RANGE window frame
      When query
        """
        SELECT o, max_by(v, o, 2) OVER (ORDER BY o RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        ORDER BY o
        """
      Then query result ordered
        | o  | r      |
        | 10 | [a]    |
        | 20 | [c, a] |
        | 30 | [d, c] |
        | 50 | [b]    |

  Rule: PIVOT

    Scenario: max_by as the PIVOT aggregate
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (max_by(v, o) FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query result ordered
        | k  | x | y    |
        | k1 | a | b    |
        | k2 | d | NULL |

    Scenario: max_by alongside another aggregate in a PIVOT
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (max_by(v, o) AS a, count(o) AS c FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query result ordered
        | k  | x_a | x_c | y_a  | y_c |
        | k1 | a   | 1   | b    | 1   |
        | k2 | d   | 2   | NULL | 0   |

    # On its general path Spark's PIVOT wraps every argument of the aggregate in
    # `IF(pivot_col <=> value, arg, NULL)` (`PivotTransformer`), `k` included, so `k` stops being
    # foldable and MaxMinByK rejects it. Sail pivots with an aggregate FILTER, keeps `k` a literal
    # and answers; reproducing the rejection is left for a follow-up.
    @sail-bug @spark-4.2
    Scenario: max_by top-k as the PIVOT aggregate
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (max_by(v, o, 2) FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query error (?s)DATATYPE_MISMATCH.NON_FOLDABLE_INPUT.*foldable int expression

    # When every aggregate's result type is supported by `PivotFirst` (here INT), Spark takes that
    # path, which does not wrap the arguments, so the top-k call inside the aggregate is accepted.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k inside a PIVOT aggregate whose result type PivotFirst supports
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (size(max_by(v, o, 2)) FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query result ordered
        | k  | x | y    |
        | k1 | 1 | 1    |
        | k2 | 2 | NULL |

  Rule: Shuffled input across partitions

    # `REPARTITION(8)` spreads the rows over several partitions, so aggregates merge partial
    # states and the plan crosses the codec when the suite runs with `SAIL_MODE=local-cluster`.
    # Window results are reduced to a checksum so that one row stands for thousands.

    Scenario: max_by over a GROUP BY on shuffled input
      When query
        """
        SELECT g, max_by(x, y * 1000000 + id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r     |
        | 0 | 13027 |
        | 1 | 19027 |
        | 2 | 18027 |
        | 3 | 17027 |
        | 4 | 16027 |
        | 5 | 15027 |
        | 6 | 14027 |

    Scenario: max_by with a DOUBLE key over a GROUP BY on shuffled input
      When query
        """
        SELECT g, max_by(x, d * 1000000 + id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r     |
        | 0 | 13027 |
        | 1 | 19027 |
        | 2 | 18027 |
        | 3 | 17027 |
        | 4 | 16027 |
        | 5 | 15027 |
        | 6 | 14027 |

    Scenario: max_by with a STRUCT key that has NULL fields on shuffled input
      When query
        """
        SELECT max_by(x, named_struct('a', st.a, 'b', st.b)) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t
        """
      Then query result ordered
        | r     |
        | 19923 |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k merges partial states on shuffled input
      When query
        """
        SELECT g, max_by(x, y * 1000000 + id, 3) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r                    |
        | 0 | [13027, 6027, 19054] |
        | 1 | [19027, 12027, 5027] |
        | 2 | [18027, 11027, 4027] |
        | 3 | [17027, 10027, 3027] |
        | 4 | [16027, 9027, 2027]  |
        | 5 | [15027, 8027, 1027]  |
        | 6 | [14027, 7027, 27]    |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k with a DOUBLE key on shuffled input
      When query
        """
        SELECT max_by(x, d * 1000000 + id, 5) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t
        """
      Then query result ordered
        | r                                   |
        | [19027, 18027, 17027, 16027, 15027] |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k with DISTINCT on shuffled input
      When query
        """
        SELECT g, max_by(DISTINCT CAST(y AS STRING), y, 3) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r               |
        | 0 | [999, 998, 997] |
        | 1 | [999, 998, 997] |
        | 2 | [999, 998, 997] |
        | 3 | [999, 998, 997] |
        | 4 | [999, 998, 997] |
        | 5 | [999, 998, 997] |
        | 6 | [999, 998, 997] |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k with FILTER on shuffled input
      When query
        """
        SELECT g, max_by(x, y * 1000000 + id, 2) FILTER (WHERE id % 3 = 0) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r              |
        | 0 | [6027, 12054]  |
        | 1 | [12027, 18054] |
        | 2 | [18027, 3054]  |
        | 3 | [3027, 9054]   |
        | 4 | [9027, 15054]  |
        | 5 | [15027, 54]    |
        | 6 | [27, 6054]     |

    @sail-bug
    Scenario: max_by in a sliding window frame on shuffled input
      When query
        """
        SELECT sum(CAST(r AS BIGINT) * id) AS h FROM (SELECT id, max_by(x, y * 1000000 + id) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h             |
        | 2664291224546 |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k in a sliding window frame on shuffled input
      When query
        """
        SELECT sum(CAST(r[0] AS BIGINT) * id + CAST(r[1] AS BIGINT)) AS h FROM (SELECT id, max_by(x, y * 1000000 + id, 2) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h             |
        | 2665740014700 |

    @sail-bug
    Scenario: max_by with a STRUCT key in a running window on shuffled input
      When query
        """
        SELECT sum(CAST(r AS BIGINT) * id) AS h FROM (SELECT id, max_by(x, st) OVER (PARTITION BY g ORDER BY id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h             |
        | 2597201908434 |

  Rule: A NULL ordering argument yields NULL

    # A literal NULL ordering means no row ever has a non-NULL ordering, so the result is NULL.
    # The existing all-NULLs scenario uses a COLUMN of NULLs; a literal takes a different path
    # through the rewrite, where an always-false filter combined with an ORDER BY on the same
    # literal used to abort the query.
    @sail-bug
    Scenario Outline: max_by returns NULL for a literal NULL ordering (<case>)
      When query
        """
        SELECT max_by(x, <ordering>) AS result FROM VALUES ('a', 10) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case      | ordering          |
        | untyped   | NULL              |
        | typed INT | CAST(NULL AS INT) |

  Rule: Ties on the ordering value

    # `MaxMinBy.updateExpressions` compares with a STRICT `old > new`, so on a tie the else
    # branch takes the NEW row: the LAST row wins. The aggregate path gets this for free because
    # it is rewritten to `last_value`, but the window path runs the accumulator, which is where
    # the rule has to be implemented rather than inherited.
    @sail-bug
    Scenario: max_by keeps the last row on a tie in a window frame
      When query
        """
        SELECT max_by(v, o) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 1, 1), ('b', 1, 2), ('c', 1, 3) AS t(v, o, i)
        """
      Then query result ordered
        | result |
        | a      |
        | b      |
        | c      |

  Rule: An argument count outside the accepted range is rejected

    # `Signature::user_defined` means DataFusion runs no arity check of its own, so
    # every hook reaching for the second argument used to index an empty/short slice
    # and panic -- which kills the gRPC connection instead of reporting an error.
    @sail-bug
    Scenario: max_by with no arguments is rejected
      When query
        """
        SELECT max_by()
        """
      Then query error (?i)max_by.*requires

    @sail-bug
    Scenario: max_by with a single argument is rejected
      When query
        """
        SELECT max_by(1)
        """
      Then query error (?i)max_by.*requires

    @sail-bug
    Scenario: max_by with four arguments is rejected
      When query
        """
        SELECT max_by(x, y, 2, 1) FROM VALUES (1, 2), (3, 4) AS t(x, y)
        """
      Then query error (?i)max_by.*requires

  Rule: IGNORE NULLS is not supported

    # `FunctionResolution.applyIgnoreNulls` has no case for MaxBy/MinBy, so it falls
    # through to `functionWithUnsupportedSyntaxError`. This is not just a missing
    # error: `MaxMinBy.updateExpressions` skips NULL ORDERINGS only and may legitimately
    # return a NULL value, whereas Sail forwards the flag to `last_value`, which skips
    # NULL VALUES and answered 'b' where the two-argument semantics say NULL.
    @sail-bug
    Scenario: max_by rejects IGNORE NULLS
      When query
        """
        SELECT max_by(v, o) IGNORE NULLS AS result
        FROM VALUES (CAST(NULL AS STRING), 3), ('b', 2), ('c', 1) AS t(v, o)
        """
      Then query error does not support IGNORE NULLS

    # `applyIgnoreNulls` runs on the function itself (the Spark stack goes through
    # `WindowExpression.mapChildren`), so the window form is rejected as well.
    @sail-bug
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

  Rule: WITHIN GROUP is not supported

    # `max_by` is not a `SupportsOrderingWithinGroup`, so Spark rejects the clause outright.
    # Accepting it is not merely a missing error: Sail's rewrite appends its own sort AFTER the
    # user's, so `z` becomes the primary key and `y` only breaks ties -- the query below answers
    # by the wrong column rather than failing.
    @sail-bug
    Scenario: max_by rejects WITHIN GROUP
      When query
        """
        SELECT max_by(x, y) WITHIN GROUP (ORDER BY z) AS result
        FROM VALUES ('a', 10, 3), ('b', 50, 2), ('c', 20, 1) AS t(x, y, z)
        """
      Then query error does not support WITHIN GROUP

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k rejects WITHIN GROUP
      When query
        """
        SELECT max_by(x, y, 2) WITHIN GROUP (ORDER BY z) AS result
        FROM VALUES ('a', 10, 3), ('b', 50, 2), ('c', 20, 1) AS t(x, y, z)
        """
      Then query error does not support WITHIN GROUP

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

    # This asserts only that DISTINCT is ACCEPTED and does not change the answer -- it cannot
    # assert the de-duplication itself, because no two-argument input can discriminate:
    # dropping duplicate (value, ordering) pairs never moves which value sits at the extreme
    # ordering. The discriminating case lives in the top-k Rule below, where de-duplication is
    # observable in the returned array.
    Scenario: max_by accepts DISTINCT
      When query
        """
        SELECT max_by(DISTINCT v, o) AS result FROM VALUES ('a', 1), ('a', 5), ('b', 3) AS t(v, o)
        """
      Then query result
        | result |
        | a      |

    # A sliding frame needs `retract_batch`, which `MaxMinByAccumulator` does not
    # implement, so Sail fails to plan the query outright.
  Rule: Ordering types Spark rejects beyond MAP

    # `OrderUtils.isOrderable` rejects MAP, VARIANT, GEOMETRY and GEOGRAPHY, and recurses through
    # ARRAY and STRUCT -- so an ARRAY of MAP is rejected too. Sail has no orderability check, and
    # for VARIANT that is worse than accepting too much: the row encoder orders the variant by the
    # bytes of its binary encoding, so `max_by` silently answers using an order with no meaning.
    # Discriminating input: '"aaa"' vs '"b"' -- a string order would pick 'b', the byte order picks 'a'.
    @sail-bug
    Scenario: max_by rejects an ARRAY of MAP ordering column
      When query
        """
        SELECT max_by(x, array(map('k', y))) AS result FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error does not support ordering on type

  Rule: Clauses that are only valid in one of the two positions

    # Spark rejects DISTINCT on a window function outright, whatever the function.
    @sail-bug
    Scenario: max_by rejects DISTINCT in a window
      When query
        """
        SELECT max_by(DISTINCT x, y) OVER (PARTITION BY y) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error Distinct window functions are not supported

    # The converse: Spark accepts and honours a FILTER on a window aggregate.
    @sail-bug
    Scenario: max_by honours FILTER in a window
      When query
        """
        SELECT max_by(x, y) FILTER (WHERE x <> 'a') OVER (PARTITION BY y) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |
        | b      |
        | c      |

  Rule: The three-argument form returns the k values at the extreme of the ordering column

    # Added in Spark 4.2 (`MaxMinByK.scala`, `MaxByBuilder` accepts [2, 3] arguments):
    # returns an ARRAY of the k values, sorted descending by the ordering column.
    # The array is joined so that the assertion stays plain SQL; the element order is
    # part of what is asserted, and it is deterministic here because every `y` differs.
    @sail-bug
    @spark-4.2
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
    @sail-bug
    @spark-4.2
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

    # `inputTypes = Seq(AnyDataType, AnyDataType, IntegerType)` with `ImplicitCastInputTypes`,
    # so `k` is implicitly cast to INT and every type that can store-assign to INT is accepted.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by accepts k written as <case>
      When query
        """
        SELECT array_join(max_by(x, y, <k>), ',') AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | b,c    |

      Examples:
        | case    | k                |
        | an INT  | 2                |
        | a BIGINT| CAST(2 AS BIGINT)|
        | a DECIMAL | 2.0            |
        | a STRING  | '2'            |

    # A NULL `k` is not a foldability failure: Scala's `null.asInstanceOf[Int]` is 0, so Spark
    # reports it through the range check with a current value of 0.
    @sail-bug
    @spark-4.2
    Scenario: max_by rejects a NULL k as out of range
      When query
        """
        SELECT max_by(x, y, NULL) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error must be between \[1, 100000\]

    # Spark's rule for `k` is FOLDABILITY, not "is a literal", so a constant-folded
    # expression qualifies and must behave exactly like the literal it folds to.
    @sail-bug
    @spark-4.2
    Scenario: max_by accepts a foldable expression as k
      When query
        """
        SELECT array_join(max_by(x, y, 1 + 1), ',') AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | b,c    |

    @sail-bug
    @spark-4.2
    Scenario: max_by rejects a non-foldable k
      When query
        """
        SELECT max_by(x, y, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error should be a foldable

    # `MaxMinByK.MAX_K` is 100000 and `k` must be at least 1.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by rejects k outside [1, 100000] (<case>)
      When query
        """
        SELECT max_by(x, y, <k>) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error must be between \[1, 100000\]

      Examples:
        | case      | k      |
        | too small | 0      |
        | too large | 100001 |

    # `checkInputDataTypes` runs at ANALYSIS, so the SHAPE of the query must not matter. The
    # rows below are the shapes a check placed further down the pipeline would miss: DataFusion
    # builds accumulators lazily per group, so a grouped query whose groups are all filtered
    # away never creates one, and the window path is a separate function registration.
    # The value axis is covered by the Outline above; this one varies only the shape.
    @sail-bug
    @spark-4.2
    Scenario Outline: max_by rejects an out-of-range k in <shape>
      When query
        """
        <query>
        """
      Then query error must be between \[1, 100000\]

      Examples:
        | shape                       | query                                                                                             |
        | an ungrouped aggregate      | SELECT max_by(x, y, 0) AS result FROM VALUES ('a', 10), ('b', 50) AS t(x, y)                      |
        | a grouped aggregate         | SELECT g, max_by(x, y, 0) AS result FROM VALUES ('g1', 'a', 1), ('g2', 'b', 2) AS t(g, x, y) GROUP BY g |
        | a group filtered away       | SELECT g, max_by(x, y, 0) AS result FROM VALUES ('g', 'a', 1) AS t(g, x, y) WHERE y > 999 GROUP BY g    |
        | a window frame              | SELECT max_by(x, y, 0) OVER (ORDER BY y) AS result FROM VALUES ('a', 10), ('b', 50) AS t(x, y)     |

    @sail-bug
    @spark-4.2
    Scenario Outline: max_by rejects a non-foldable k in <shape>
      When query
        """
        <query>
        """
      Then query error should be a foldable

      Examples:
        | shape                  | query                                                                                                |
        | an ungrouped aggregate | SELECT max_by(x, y, y) AS result FROM VALUES ('a', 10), ('b', 50) AS t(x, y)                         |
        | a group filtered away  | SELECT g, max_by(x, y, y) AS result FROM VALUES ('g', 'a', 1) AS t(g, x, y) WHERE y > 999 GROUP BY g |
        | a window frame         | SELECT max_by(x, y, y) OVER (ORDER BY y) AS result FROM VALUES ('a', 10), ('b', 50) AS t(x, y)       |

    # `inputTypes` pins the third slot to INT, and implicit casting only reaches types that can
    # store-assign to INT. DATE is not one of them, so Spark rejects at analysis rather than
    # reinterpreting the date as a day count.
    @spark-4.2 @sail-bug
    Scenario: max_by rejects a DATE k
      When query
        """
        SELECT max_by(x, y, DATE '2024-01-01') AS result FROM VALUES ('a', 10) AS t(x, y)
        """
      Then query error requires the "INT" type

    # The builders declare no `functionSignature`, so named arguments are rejected outright.
    @spark-4.2 @sail-bug
    Scenario: max_by rejects a named k argument
      When query
        """
        SELECT max_by(x, y, k => 2) AS result FROM VALUES ('a', 10) AS t(x, y)
        """
      Then query error Named parameters are not supported

    # `k` is cast to INT before the range check, so a malformed or overflowing value fails the
    # cast rather than the range test, with the cast's own error class.
    @spark-4.2 @sail-bug
    Scenario Outline: max_by reports a cast failure for k written as <case>
      When query
        """
        SELECT max_by(x, y, <k>) AS result FROM VALUES ('a', 10) AS t(x, y)
        """
      Then query error <error>

      Examples:
        | case         | k          | error              |
        | a bad string | 'two'      | CAST_INVALID_INPUT |
        | an overflow  | 2147483648 | CAST_OVERFLOW      |

    # The accumulator is built lazily per partition, so a window over a partition that survives
    # no row would never reach a check that lives there.
    @spark-4.2 @sail-bug
    Scenario: max_by rejects an out-of-range k in a window over an empty partition
      When query
        """
        SELECT max_by(x, y, 0) OVER (PARTITION BY y) AS result
        FROM VALUES ('a', 10) AS t(x, y) WHERE y > 999
        """
      Then query error must be between \[1, 100000\]

    # The upper boundary is valid, and a MAP is legal as the VALUE argument even though it is
    # rejected as the ordering one.
    @sail-bug
    @spark-4.2
    Scenario: max_by accepts k at the maximum
      When query
        """
        SELECT size(max_by(x, y, 100000)) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k accepts a MAP value argument
      When query
        """
        SELECT size(max_by(map('k', x), y, 2)) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | 2      |

    # A `k` larger than the group returns every row, not a padded array.
    @sail-bug
    @spark-4.2
    Scenario: max_by returns every value when k exceeds the row count
      When query
        """
        SELECT array_join(max_by(x, y, 10), ',') AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | b,c,a  |

    # An empty group yields NULL rather than an empty array (`eval` returns null when the
    # heap is empty), and NULL orderings are skipped while NULL VALUES are kept.
    @spark-4.2
    Scenario: max_by returns NULL for an empty group
      When query
        """
        SELECT max_by(x, y, 2) AS result FROM VALUES ('a', 10), ('b', 50) AS t(x, y) WHERE y > 999
        """
      Then query result
        | result |
        | NULL   |

    # The three-argument form composes with the other aggregate clauses exactly as the
    # two-argument one does.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k composes with FILTER
      When query
        """
        SELECT array_join(max_by(x, y, 2) FILTER (WHERE x <> 'a'), ',') AS result
        FROM VALUES ('a', 99), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | b,c    |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k works per group
      When query
        """
        SELECT g, array_join(max_by(x, y, 2), ',') AS result
        FROM VALUES ('g1', 'a', 1), ('g1', 'b', 2), ('g2', 'c', 3) AS t(g, x, y)
        GROUP BY g
        """
      Then query result
        | g  | result |
        | g1 | b,a    |
        | g2 | c      |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k accepts an ARRAY ordering column
      When query
        """
        SELECT array_join(max_by(v, o, 2), ',') AS result
        FROM VALUES ('lo', array(1, 2)), ('hi', array(3, 4)) AS t(v, o)
        """
      Then query result
        | result |
        | hi,lo  |

    # `OrderUtils.isOrderable` recurses through ARRAY and STRUCT, so a NESTED ordering column is
    # orderable and must be compared element-wise. These are the cases a shallow `array(1, 2)`
    # scenario cannot reach: Arrow refuses to compare nested elements directly, so a comparator
    # built on `ScalarValue::partial_cmp` reports "incomparable" and silently degenerates to
    # "all equal", which returns the FIRST k rows in arrival order.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k orders by a nested ARRAY ordering column
      When query
        """
        SELECT array_join(max_by(v, o, 2), ',') AS result
        FROM VALUES ('a', array(array(1))), ('b', array(array(3))), ('c', array(array(2))) AS t(v, o)
        """
      Then query result
        | result |
        | b,c    |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k orders by a STRUCT ordering column holding an array
      When query
        """
        SELECT array_join(max_by(v, o, 2), ',') AS result
        FROM VALUES ('a', named_struct('s', array(1))), ('b', named_struct('s', array(3))), ('c', named_struct('s', array(2))) AS t(v, o)
        """
      Then query result
        | result |
        | b,c    |

    # Spark orders a NULL element inside an array as the SMALLEST; DataFusion's own
    # `ScalarValue` comparison follows Postgres and makes it the GREATEST.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k orders a NULL array element as the smallest
      When query
        """
        SELECT array_join(max_by(v, o, 1), ',') AS result
        FROM VALUES ('a', array(CAST(NULL AS INT))), ('b', array(1)) AS t(v, o)
        """
      Then query result
        | result |
        | b      |

    @sail-bug
    @spark-4.2
    Scenario: max_by top-k orders a NULL struct field as the smallest
      When query
        """
        SELECT array_join(max_by(v, o, 1), ',') AS result
        FROM VALUES ('a', named_struct('x', CAST(NULL AS INT))), ('b', named_struct('x', 1)) AS t(v, o)
        """
      Then query result
        | result |
        | b      |

    # `checkForOrderingExpr` applies to the top-k form too, so the MAP rule is not specific to
    # the two-argument shape. Sail has no orderability check on either path.
    @spark-4.2 @sail-bug
    Scenario: max_by top-k rejects a MAP ordering column
      When query
        """
        SELECT max_by(v, o, 2) AS result FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error does not support ordering on type

    # Spark de-duplicates the aggregate's children before applying the function.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k supports DISTINCT
      When query
        """
        SELECT array_join(max_by(DISTINCT x, y, 2), ',') AS result
        FROM VALUES ('a', 10), ('a', 10), ('b', 5) AS t(x, y)
        """
      Then query result
        | result |
        | a,b    |

    # `MaxMinByK` is an `ImperativeAggregate`, which Spark's `AggregateProcessor` supports as a
    # window function. Sail reaches the same place through a real accumulator, which -- unlike a
    # `simplify` rewrite -- DataFusion also uses on the window path.
    @sail-bug
    @spark-4.2
    Scenario: max_by top-k works as a window function
      When query
        """
        SELECT array_join(max_by(x, y, 2) OVER (ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), ',') AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | a      |
        | c,a    |
        | b,c    |

    @sail-bug
    @spark-4.2
    Scenario: max_by skips NULL orderings but keeps NULL values
      When query
        """
        SELECT max_by(x, y, 2) AS result FROM VALUES (CAST(NULL AS STRING), 9), ('b', 5), ('c', CAST(NULL AS INT)) AS t(x, y)
        """
      Then query result collected
        | result      |
        | [None, 'b'] |
