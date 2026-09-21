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

  Rule: The ordering argument must be of an orderable type

    # Spark checks this in analysis: MaxMinBy.checkInputDataTypes delegates to
    # TypeUtils.checkForOrderingExpr, which rejects MAP, VARIANT and any nested
    # type containing one of them. Only the ordering argument is restricted.

    Scenario: min_by rejects a MAP ordering column
      When query
        """
        SELECT min_by(v, o) AS result
        FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error (?s)min_by.*does not support ordering on type

    Scenario: min_by rejects a MAP ordering literal
      When query
        """
        SELECT min_by(1, map('a', 1)) AS result
        """
      Then query error (?s)min_by.*does not support ordering on type

    Scenario: min_by rejects a MAP ordering column as a window function
      When query
        """
        SELECT min_by(v, map('a', i)) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(v, i)
        """
      Then query error (?s)min_by.*does not support ordering on type

    @spark-4
    Scenario: min_by rejects a VARIANT ordering column
      When query
        """
        SELECT min_by(x, parse_json(j)) AS result
        FROM VALUES ('a', '"aaa"'), ('b', '"b"') AS t(x, j)
        """
      Then query error (?s)min_by.*does not support ordering on type

    Scenario: min_by rejects an ARRAY<MAP> ordering column
      When query
        """
        SELECT min_by(x, array(map('k', y))) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error (?s)min_by.*does not support ordering on type

    # The check is recursive, so a MAP or VARIANT buried at any depth also fails.
    Scenario Outline: min_by rejects a nested <case> ordering column
      When query
        """
        SELECT min_by(x, <ordering>) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(x, y)
        """
      Then query error (?s)min_by.*does not support ordering on type

      Examples:
        | case            | ordering              |
        | STRUCT<MAP>     | struct(map('k', y))   |

    # parse_json is Spark 4.0+, so the VARIANT rows are split out of the outline above to keep
    # the MAP coverage available when the suite runs against the 3.5 oracle.
    @spark-4
    Scenario Outline: min_by rejects a nested Spark 4 <case> ordering column
      When query
        """
        SELECT min_by(x, <ordering>) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(x, y)
        """
      Then query error (?s)min_by.*does not support ordering on type

      Examples:
        | case            | ordering              |
        | STRUCT<VARIANT> | struct(parse_json(x)) |

    # Spark's CalendarIntervalType is not an AtomicType, so it is not orderable.
    Scenario: min_by rejects a calendar INTERVAL ordering column
      When query
        """
        SELECT min_by(x, make_interval(0, 0, 0, y)) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error (?s)min_by.*does not support ordering on type

    # The rejections above only discriminate if the accepted types are pinned too: a check that
    # rejected every type would satisfy every scenario in this Rule. These are the orderable
    # counterparts, including the ANSI intervals the calendar one above is contrasted with.
    # The winning row is deliberately in the MIDDLE of the input: Sail rewrites min_by to an
    # ordered `last_value(x)`, so a fixture whose minimum is also its first or last row would
    # stay green even if the ordering key were dropped entirely.
    Scenario Outline: min_by accepts an orderable <case> ordering column
      When query
        """
        SELECT min_by(x, <ordering>) AS result
        FROM VALUES ('hi', 3), ('lo', 1), ('mid', 2) AS t(x, y)
        """
      Then query result
        | result |
        | lo     |

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
    Scenario: min_by accepts a VOID ordering column and returns NULL
      When query
        """
        SELECT min_by(x, CAST(NULL AS VOID)) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |

    # OrderUtils.isOrderable rejects GEOMETRY and GEOGRAPHY explicitly, before the AtomicType
    # case, because they are opaque WKB bytes with no meaningful ordering. Sail lowers both to
    # plain BINARY and keeps the geo identity in the field metadata, so `coerce_types` cannot
    # see it; the check is repeated in `return_field`, which does receive the fields. So Sail answers by ordering the raw bytes,
    # which is why it returns a value here instead of failing.
    @spark-4.2
    Scenario: min_by rejects a GEOMETRY ordering column
      When query
        """
        SELECT min_by(v, st_geomfromwkb(w)) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)min_by.*does not support ordering on type

    # Spark recurses into ARRAY and STRUCT, so a nested GEOMETRY is unorderable too. The check
    # only sees it if `array()` and `named_struct()` keep the child's geo metadata.
    @spark-4.2
    Scenario Outline: min_by rejects a GEOMETRY nested by <case>
      When query
        """
        SELECT min_by(v, <ordering>) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)min_by.*does not support ordering on type

      Examples:
        | case         | ordering                             |
        | array        | array(st_geomfromwkb(w))             |
        | named_struct | named_struct('g', st_geomfromwkb(w)) |

  Rule: Clause surface

    # These are not orderability rules: Spark decides them in FunctionResolution.validateFunction
    # and in CheckAnalysis, for the function itself and in either position (aggregate or window).

    Scenario Outline: min_by accepts the clause <case>
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
        | plain    | min_by(x, y)                       | a      |
        | DISTINCT | min_by(DISTINCT x, y)              | a      |
        | FILTER   | min_by(x, y) FILTER (WHERE y > 10) | c      |

    # A window aggregate emits one row per input row, so this returns four.
    Scenario: min_by accepts the clause OVER
      When query
        """
        SELECT min_by(x, y) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query result
        | result |
        | a      |
        | a      |
        | a      |
        | a      |

    @spark-4
    Scenario: min_by rejects the clause IGNORE NULLS
      When query
        """
        SELECT min_by(x, y) IGNORE NULLS AS result
        FROM VALUES (CAST(NULL AS STRING), 5), ('b', 10) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support IGNORE NULLS

    @spark-4
    Scenario: min_by rejects the clause WITHIN GROUP
      When query
        """
        SELECT min_by(x, y) WITHIN GROUP (ORDER BY x DESC) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support WITHIN GROUP

    Scenario: min_by rejects the clause DISTINCT combined with OVER
      When query
        """
        SELECT min_by(DISTINCT x, y) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error DISTINCT_WINDOW_FUNCTION_UNSUPPORTED

    @spark-4.2
    Scenario: min_by supports the clause FILTER combined with OVER
      When query
        """
        SELECT min_by(x, y) FILTER (WHERE y > 10) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | c      |
        | c      |
        | c      |

    # Spark names an unaliased aggregate with its FILTER clause, as `<call> FILTER (WHERE <condition>)`.
    Scenario: min_by names an unaliased result with its FILTER clause
      When query
        """
        SELECT min_by(x, y) FILTER (WHERE y > 10)
        FROM VALUES ('a', 10), ('b', 50) AS t(x, y)
        """
      Then query schema
        """
        root
         |-- min_by(x, y) FILTER (WHERE (y > 10)): string (nullable = true)
        """

  Rule: Arity

    # Spark's MaxMinBy is BinaryLike. Sail used to panic here and kill the RPC, because
    # Signature::user_defined skips DataFusion's arity gate and coerce_types is the only one.
    Scenario Outline: min_by rejects a call with <case>
      When query
        """
        SELECT min_by(<args>)
        """
      Then query error (?si)min_by.*requires

      Examples:
        | case            | args       |
        | no arguments    |            |
        | one argument    | 1          |
        | four arguments  | 1, 2, 3, 4 |

    # Spark 4.2 added the top-k form, which returns an array of the k values
    # (MaxMinByK.scala).
    @spark-4.2
    Scenario: min_by supports the three-argument top-k form
      When query
        """
        SELECT min_by(x, y, 2) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result   |
        | [a, c]   |

  Rule: Constant and untyped ordering keys

    # A constant ordering key makes every row tie, and Spark documents the winner among tied
    # rows as unspecified (`MaxByAndMinBy.scala`: "the output can be different for those
    # associated the same values"), because partial-state merge order is not guaranteed. So
    # assert only that the call is accepted and returns one of the tied values; the running
    # window scenarios above pin the directional tie rule where it IS deterministic.
    # Foldable arguments are constant-folded to the same literal.
    Scenario Outline: min_by accepts the constant ordering key <case>
      When query
        """
        SELECT min_by(x, <ordering>) IN ('a', 'b', 'c') AS result
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
    Scenario: min_by takes the newer row on ties in a running window
      When query
        """
        SELECT i,
               min_by(x, 1) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(i, x)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | b      |
        | 3 | c      |

    # NullType is orderable in Spark, which returns NULL here.
    Scenario: min_by accepts an untyped NULL ordering argument
      When query
        """
        SELECT min_by(x, NULL) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query result
        | result |
        | NULL   |

  Rule: The value argument has no orderability restriction

    # This is the other half of "only the ordering argument is restricted", claimed by the
    # header of the orderability Rule above: `MaxMinBy.checkInputDataTypes` only looks at
    # `orderingExpr`, and `dataType` is `valueExpr.dataType` whatever that is. Winning row in
    # the middle again, for the reason given on the orderable table above.
    Scenario Outline: min_by accepts a <case> value argument
      When query
        """
        SELECT <value> AS result
        FROM VALUES ('{"v":3}', 3), ('{"v":1}', 1), ('{"v":2}', 2) AS t(j, y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | value                                      | result  |
        | MAP         | min_by(map('a', y), y)['a']                | 1       |
        | ARRAY<MAP>  | min_by(array(map('k', y)), y)[0]['k']      | 1       |
        | STRUCT<MAP> | min_by(struct(map('k', y) AS m), y).m['k'] | 1       |

    @spark-4
    Scenario: min_by accepts a VARIANT value argument
      When query
        """
        SELECT to_json(min_by(parse_json(j), y)) AS result
        FROM VALUES ('{"v":3}', 3), ('{"v":1}', 1), ('{"v":2}', 2) AS t(j, y)
        """
      Then query result
        | result  |
        | {"v":1} |

  Rule: Spark ordering semantics of the ordering argument

    # `MaxByAndMinBy.scala` updates with `If(old < new, old, new)`, so equal keys take the newer
    # row. The comparison is SQL ordering, where -0.0 equals 0.0, so a running window must switch
    # to the newer row. An IEEE total order would rank -0.0 below 0.0 and keep the older row.
    Scenario: min_by treats negative zero and zero DOUBLE ordering keys as equal in a running window
      When query
        """
        SELECT i,
               min_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'neg', -0.0D), (2, 'pos', 0.0D) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | neg    |
        | 2 | pos    |

    # In Spark's nested ordering a NULL array element is smaller than a non-null one, so the
    # newer `array(NULL)` wins over `array(1)`. DataFusion's `ScalarValue::partial_cmp` follows
    # Postgres instead and ranks the NULL element greater.
    Scenario: min_by ranks a NULL array element below a non-null one in a running window
      When query
        """
        SELECT i,
               min_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'one', array(1)), (2, 'null', array(CAST(NULL AS INT))) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | one    |
        | 2 | null   |

    # The same rule decides the aggregate form. A descending sort with NULLS FIRST would also put
    # the nested NULL first, ranking it as the largest value, so the non-null row would win.
    Scenario: min_by ranks a NULL struct field below a non-null one in an aggregate
      When query
        """
        SELECT min_by(x, y) AS result
        FROM VALUES ('one', named_struct('a', 1)), ('null', named_struct('a', CAST(NULL AS INT))) AS t(x, y)
        """
      Then query result
        | result |
        | null   |

    # In the window form, DataFusion's `partial_cmp_struct` skips the NULL position and answers
    # `Equal`, which would turn the comparison into a tie, so both row orders are pinned.
    Scenario Outline: min_by ranks a NULL struct field below a non-null one in a running window with <case>
      When query
        """
        SELECT i,
               min_by(x, y) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, '<first>', named_struct('a', <first_key>)), (2, '<second>', named_struct('a', <second_key>)) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result   |
        | 1 | <first>  |
        | 2 | <result> |

      Examples:
        | case                 | first | first_key         | second | second_key        | result |
        | the NULL field newer | one   | 1                 | null   | CAST(NULL AS INT) | null   |
        | the NULL field older | null  | CAST(NULL AS INT) | one    | 1                 | null   |

  Rule: The value argument keeps its logical type

    # `dataType = valueExpr.dataType`, so the result is still a GEOGRAPHY. Sail carries GEOGRAPHY
    # as BINARY plus field metadata, so the result field must be the value field, not one rebuilt
    # from its `DataType`.
    @spark-4.2
    Scenario: min_by keeps the GEOGRAPHY type of the value argument
      When query
        """
        SELECT min_by(st_geogfromwkb(w), y) AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040') AS t(y, w)
        """
      Then query schema
        """
        root
         |-- result: geography (nullable = true)
        """

  Rule: Spark ordering boundaries that Sail already matches

    # These keys all have a strict winner, so both the aggregate and the whole-partition window
    # form are deterministic. Each boundary is one Sail could get wrong: NaN is the largest double
    # in Spark's SQL ordering, BINARY compares unsigned bytes, a pre-epoch fraction must not flip
    # sign, DECIMAL(38,0) sits at the 128-bit limit, and a longer array wins over its own prefix.
    Scenario Outline: min_by follows Spark ordering for <case> ordering keys in the <path> form
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
        | NaN above infinity             | aggregate | min_by(x, o)         | ('one', 1.0D), ('inf', CAST('Infinity' AS DOUBLE)), ('nan', CAST('NaN' AS DOUBLE)), ('ninf', CAST('-Infinity' AS DOUBLE))  | ninf   |
        | NaN above infinity             | window    | min_by(x, o) OVER () | ('one', 1.0D), ('inf', CAST('Infinity' AS DOUBLE)), ('nan', CAST('NaN' AS DOUBLE)), ('ninf', CAST('-Infinity' AS DOUBLE))  | ninf   |
        | unsigned BINARY                | aggregate | min_by(x, o)         | ('lo', X'01'), ('hi', X'FF'), ('mid', X'7F')                                                                               | lo     |
        | unsigned BINARY                | window    | min_by(x, o) OVER () | ('lo', X'01'), ('hi', X'FF'), ('mid', X'7F')                                                                               | lo     |
        | pre-epoch fractional TIMESTAMP | aggregate | min_by(x, o)         | ('a', TIMESTAMP '1969-12-31 23:59:59.5'), ('b', TIMESTAMP '1969-12-31 23:59:59.9'), ('c', TIMESTAMP '1969-12-31 23:59:59') | c      |
        | pre-epoch fractional TIMESTAMP | window    | min_by(x, o) OVER () | ('a', TIMESTAMP '1969-12-31 23:59:59.5'), ('b', TIMESTAMP '1969-12-31 23:59:59.9'), ('c', TIMESTAMP '1969-12-31 23:59:59') | c      |
        | DECIMAL(38,0) extremes         | aggregate | min_by(x, o)         | ('max', 99999999999999999999999999999999999999BD), ('min', -99999999999999999999999999999999999999BD), ('zero', 0BD)       | min    |
        | DECIMAL(38,0) extremes         | window    | min_by(x, o) OVER () | ('max', 99999999999999999999999999999999999999BD), ('min', -99999999999999999999999999999999999999BD), ('zero', 0BD)       | min    |
        | ARRAY prefix length            | aggregate | min_by(x, o)         | ('short', array(1, 2)), ('long', array(1, 2, 0)), ('big', array(0, 9))                                                     | big    |
        | ARRAY prefix length            | window    | min_by(x, o) OVER () | ('short', array(1, 2)), ('long', array(1, 2, 0)), ('big', array(0, 9))                                                     | big    |

    # NaN == NaN in Spark's SQL ordering, so every row ties and the strict predicate takes the
    # newer one. `ScalarValue::partial_cmp` agrees here because `total_cmp` also equates NaNs.
    Scenario: min_by takes the newer row on NaN ties in a running window
      When query
        """
        SELECT i,
               min_by(x, o) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
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
    Scenario Outline: min_by treats negative zero and zero as equal inside a STRUCT ordering key in the <path> form
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
        | aggregate | min_by(x, o)         | pos    |
        | window    | min_by(x, o) OVER () | pos    |

    # A NULL ordering key is skipped, and a VOID key is NULL on every row, so Spark returns NULL.
    # A `NullArray` has no validity buffer, so the window accumulator must read its logical nulls.
    Scenario: min_by skips a VOID ordering key in a running window
      When query
        """
        SELECT i,
               min_by(x, CAST(NULL AS VOID)) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a'), (2, 'b') AS t(i, x)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | NULL   |

    # The aggregate counterpart of the array window scenario above: a descending sort with NULLS
    # FIRST would rank the NULL element as the largest, so the non-null rows would win.
    Scenario: min_by ranks a NULL array element below a non-null one in an aggregate
      When query
        """
        SELECT min_by(x, o) AS result
        FROM VALUES ('one', array(1)), ('null', array(CAST(NULL AS INT))), ('zero', array(0)) AS t(x, o)
        """
      Then query result
        | result |
        | null   |

    # The window form reaches the same `return_field`, so it keeps the GEOGRAPHY metadata too.
    @spark-4.2
    Scenario: min_by keeps the GEOGRAPHY type of the value argument as a window function
      When query
        """
        SELECT min_by(st_geogfromwkb(w), y) OVER () AS result
        FROM VALUES (1, X'0101000000000000000000F03F0000000000000040') AS t(y, w)
        """
      Then query schema
        """
        root
         |-- result: geography (nullable = true)
        """

  Rule: Aggregate surface

    # Sail rewrites the aggregate form to an ordered `last_value` with an added
    # `ordering IS NOT NULL` filter, so these pin the shapes that rewrite has to survive. A NULL
    # value is not skipped: only a NULL ordering key is, so a NULL at the extremum is the answer.
    # The code-point case holds because Spark compares UTF-8 bytes, not UTF-16 code units.
    Scenario Outline: min_by returns the expected value for <case>
      When query
        """
        <query>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | query                                                                                                                                                                | result |
        | empty input                                      | SELECT min_by(x, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y) WHERE false                                                                     | NULL   |
        | single row                                       | SELECT min_by(x, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y) WHERE x = 'c'                                                                   | c      |
        | a NULL value at the extremum                     | SELECT min_by(CASE WHEN y = 10 THEN NULL ELSE x END, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                             | NULL   |
        | a negated ordering expression                    | SELECT min_by(x, -y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                                | b      |
        | a CASE ordering expression                       | SELECT min_by(x, CASE WHEN x = 'c' THEN 0 ELSE y END) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                               | c      |
        | the value and ordering being the same column     | SELECT min_by(y, y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                                 | 10     |
        | a constant value                                 | SELECT min_by('k', y) AS result FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)                                                                               | k      |
        | an ordering column from a joined table           | SELECT min_by(a.x, b.w) AS result FROM VALUES ('a', 1), ('b', 2), ('c', 3) AS a(x, id) JOIN VALUES (1, 30), (2, 10), (3, 20) AS b(id, w) ON a.id = b.id              | b      |
        | a CTE                                            | WITH s AS (SELECT * FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)) SELECT min_by(x, y) AS result FROM s                                                     | a      |
        | try_cast NULLs in the ordering                   | SELECT min_by(x, try_cast(o AS INT)) AS result FROM VALUES ('a', '1'), ('b', 'zz'), ('c', '3') AS t(x, o)                                                            | a      |
        | long strings sharing a prefix                    | SELECT min_by(x, o) AS result FROM VALUES ('short', repeat('a', 5000)), ('long', concat(repeat('a', 5000), 'b')), ('mid', concat(repeat('a', 4999), 'b')) AS t(x, o) | short  |
        | a supplementary character against a high BMP one | SELECT min_by(x, o) AS result FROM VALUES ('emoji', '😀'), ('bmp', '｡') AS t(x, o)                                                                                    | bmp    |

    # The FILTER predicate is ANDed with the rewrite's own NULL filter, per group.
    Scenario: min_by combines FILTER with a GROUP BY that has a NULL group key
      When query
        """
        SELECT g, min_by(x, y) FILTER (WHERE i > 1) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        GROUP BY g
        ORDER BY g NULLS FIRST
        """
      Then query result ordered
        | g    | result |
        | NULL | c      |
        | g1   | b      |
        | g2   | e      |

    Scenario: min_by works with GROUPING SETS
      When query
        """
        SELECT g, min_by(x, y) AS result, grouping(g) AS gg
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        GROUP BY GROUPING SETS ((g), ())
        ORDER BY gg, g NULLS FIRST
        """
      Then query result ordered
        | g    | result | gg |
        | NULL | c      | 0  |
        | g1   | a      | 0  |
        | g2   | e      | 0  |
        | NULL | a      | 1  |

  Rule: Window surface

    # A NULL partition key forms its own partition, and a named WINDOW resolves like an inline one.
    Scenario: min_by over a named window partitioned by a key with NULLs
      When query
        """
        SELECT i, min_by(x, y) OVER w AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        WINDOW w AS (PARTITION BY g)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | a      |
        | 3 | c      |
        | 4 | c      |
        | 5 | e      |

    # With ORDER BY and no frame, the default frame is RANGE up to the current row, so ORDER BY
    # peers (here the NULL keys, sorted first) are all inside each other's frame.
    Scenario: min_by includes ORDER BY peers in the default window frame
      When query
        """
        SELECT i, min_by(x, y) OVER (ORDER BY g) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | a      |
        | 3 | c      |
        | 4 | c      |
        | 5 | a      |

    # Spark's update expression keeps NULL while no row has had a non-null key yet, and then
    # ignores rows whose key is NULL.
    Scenario: min_by skips NULL ordering keys in a running window
      When query
        """
        SELECT i,
               min_by(x, CASE WHEN i IN (1, 3) THEN NULL ELSE y END) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | NULL   |
        | 2 | b      |
        | 3 | b      |
        | 4 | d      |
        | 5 | e      |

    # Any frame that does not start at UNBOUNDED PRECEDING needs a retractable accumulator: at
    # row 4 the buffered minimum leaves the frame and the next best row has to take over.
    Scenario: min_by supports a sliding window frame
      When query
        """
        SELECT i, min_by(x, y) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('a', 10, 'g1', 1), ('b', 50, 'g1', 2), ('c', 20, NULL, 3), ('d', 40, NULL, 4), ('e', 30, 'g2', 5) AS t(x, y, g, i)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | a      |
        | 2 | a      |
        | 3 | c      |
        | 4 | c      |
        | 5 | e      |

    # A frame that reaches FOLLOWING rows retracts from its front while rows enter at its back.
    # NULL keys enter and leave it without ever winning, and at row 5 `e` and `f` tie on 2, so the newer `f` wins.
    Scenario: min_by retracts ties and NULL keys in a sliding window frame
      When query
        """
        SELECT i, min_by(x, y) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS result
        FROM VALUES (1, 'a', 3), (2, 'b', 1), (3, 'c', 3), (4, 'd', CAST(NULL AS INT)), (5, 'e', 2), (6, 'f', 2), (7, 'g', 5), (8, 'h', CAST(NULL AS INT)) AS t(i, x, y)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | b      |
        | 2 | b      |
        | 3 | b      |
        | 4 | b      |
        | 5 | f      |
        | 6 | f      |
        | 7 | f      |
        | 8 | f      |

  Rule: Named arguments

    # `MinByBuilder` is a plain ExpressionBuilder with no `functionSignature`, so Spark
    # rejects named arguments in analysis.
    Scenario: min_by rejects named arguments
      When query
        """
        SELECT min_by(x => x, y => y) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query error NAMED_PARAMETERS_NOT_SUPPORTED

    # The named-argument check applies to known built-ins only: Spark resolves the routine first,
    # so a misspelled one is reported as UNRESOLVED_ROUTINE, never as named-argument misuse.
    Scenario: a misspelled min_by called with named arguments is not reported as named-argument misuse
      When query
        """
        SELECT min_byy(x => 'a', y => 1) AS result
        """
      Then query error (?s)\A(?!.*NAMED_PARAMETERS_NOT_SUPPORTED).*min_byy

  Rule: Output schema

    # `dataType = valueExpr.dataType` and `nullable = true`, whatever the value's own nullability.
    Scenario Outline: min_by returns a nullable result for a <case> value argument
      When query
        """
        SELECT min_by(<value>, y) AS result
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
    Scenario: min_by returns a nullable result for a VOID value argument
      When query
        """
        SELECT min_by(NULL, y) AS result
        FROM VALUES ('a', 10, 1), ('b', 50, 2), ('c', 20, 3) AS t(x, y, i)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    # The nested nullability flags come from the value argument unchanged.
    Scenario: min_by keeps the nested nullability of an ARRAY value argument
      When query
        """
        SELECT min_by(array(i, NULL), y) OVER () AS result
        FROM VALUES ('a', 10, 1), ('b', 50, 2), ('c', 20, 3) AS t(x, y, i)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """

  Rule: The top-k form

    # `MaxMinByK` returns the values of the k rows with the smallest orderings, sorted ascending
    # by the ordering, as `ARRAY<value type>`. It skips NULL orderings but keeps NULL values,
    # returns NULL when no ordering is non-null, and casts k to INT. These inputs have no ties
    # inside the first k rows, so the order of each array is deterministic.
    @spark-4.2
    Scenario Outline: min_by returns the top-k values for <case>
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
        | k of 2                              | min_by(x, y, 2)                         |             | [a, c]             |
        | k above the row count               | min_by(x, y, 10)                        |             | [a, c, f, NULL, b] |
        | k of 1                              | min_by(x, y, 1)                         |             | [a]                |
        | k as a string                       | min_by(x, y, '2')                       |             | [a, c]             |
        | k as a double                       | min_by(x, y, 2.0)                       |             | [a, c]             |
        | k as a foldable expression          | min_by(x, y, 1 + 1)                     |             | [a, c]             |
        | k at the maximum                    | min_by(x, y, 100000)                    |             | [a, c, f, NULL, b] |
        | no input rows                       | min_by(x, y, 2)                         | WHERE false | NULL               |
        | only NULL orderings                 | min_by(x, CAST(NULL AS INT), 2)         |             | NULL               |
        | FILTER                              | min_by(x, y, 2) FILTER (WHERE g = 'g2') |             | [c, f]             |
        | a STRUCT ordering with a NULL field | min_by(x, named_struct('a', y), 3)      |             | [d, a, c]          |

    # DISTINCT removes duplicate (value, ordering) pairs before the k rows are chosen.
    @spark-4.2
    Scenario: min_by top-k removes duplicate pairs with DISTINCT
      When query
        """
        SELECT min_by(DISTINCT x, y, 2) AS result
        FROM VALUES ('a', 1), ('a', 1), ('b', 2), ('b', 2), ('c', 0) AS t(x, y)
        """
      Then query result
        | result |
        | [c, a] |

    @spark-4.2
    Scenario: min_by top-k is computed per group
      When query
        """
        SELECT g, min_by(x, y, 2) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g  | result    |
        | g1 | [a, NULL] |
        | g2 | [c, f]    |

    @spark-4.2
    Scenario: min_by top-k in a running window
      When query
        """
        SELECT i, min_by(x, y, 2) OVER (ORDER BY i) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        ORDER BY i
        """
      Then query result ordered
        | i | result |
        | 1 | [a]    |
        | 2 | [a, b] |
        | 3 | [a, c] |
        | 4 | [a, c] |
        | 5 | [a, c] |
        | 6 | [a, c] |

    # A sliding frame has to drop rows as they leave, including the top values themselves.
    @spark-4.2
    Scenario: min_by top-k in a sliding window frame
      When query
        """
        SELECT i, min_by(x, y, 2) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        ORDER BY i
        """
      Then query result ordered
        | i | result    |
        | 1 | [a]       |
        | 2 | [a, b]    |
        | 3 | [a, c]    |
        | 4 | [c, b]    |
        | 5 | [c, NULL] |
        | 6 | [f, NULL] |

    @spark-4.2
    Scenario: min_by top-k returns an array of nullable values
      When query
        """
        SELECT min_by(x, y, 2) AS result
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
    @spark-4.2
    Scenario Outline: min_by top-k rejects <case>
      When query
        """
        SELECT min_by(x, y, <k>) AS result
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
    @spark-4.2
    Scenario: min_by top-k rejects an out-of-range k when no group survives
      When query
        """
        SELECT g, min_by(x, y, 0) AS result
        FROM VALUES (1, 'a', 10, 'g1'), (2, 'b', 50, 'g1'), (3, 'c', 20, 'g2'), (4, 'd', CAST(NULL AS INT), 'g2'), (5, CAST(NULL AS STRING), 40, 'g1'), (6, 'f', 30, 'g2') AS t(i, x, y, g)
        WHERE false
        GROUP BY g
        """
      Then query error The .k. must be between .1, 100000. .current value = 0.

  Rule: Rejections carry Spark's error class

    # Spark reports these in analysis with an error class; clients match on the class rather than
    # on the free text, so the class name is asserted alongside the message core.
    @spark-4.2
    Scenario Outline: min_by reports the Spark error class for <case>
      When query
        """
        SELECT <call> AS result
        FROM VALUES (1, 'a', 10), (2, 'b', 50) AS t(i, x, y)
        """
      Then query error <error>

      Examples:
        | case                  | call                            | error                                                                          |
        | an unorderable key    | min_by(x, map('k', y))          | (?s)DATATYPE_MISMATCH.INVALID_ORDERING_TYPE.*does not support ordering on type |
        | an out-of-range k     | min_by(x, y, 0)                 | (?s)DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE.*The .k. must be between              |
        | a non-foldable k      | min_by(x, y, i)                 | (?s)DATATYPE_MISMATCH.NON_FOLDABLE_INPUT.*foldable int expression              |
        | a k of the wrong type | min_by(x, y, DATE '2024-01-01') | (?s)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE.*requires the "INT" type           |
        | four arguments        | min_by(x, y, 2, 3)              | (?s)WRONG_NUM_ARGS.*requires .2, 3. parameters                                 |

    # A GEOMETRY key is unorderable however it is produced. Sail's CASE builds its output field
    # from the branch `DataType` alone (DataFusion's `expr_schema.rs`), dropping the geo metadata,
    # so the key reaches the orderability check as plain BINARY and is accepted.
    @sail-bug @spark-4.2
    Scenario: min_by rejects a GEOMETRY ordering key produced by CASE
      When query
        """
        SELECT min_by(v, CASE WHEN true THEN st_geomfromwkb(w) END) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error (?s)min_by.*does not support ordering on type

  Rule: Grouping extensions

    Scenario: min_by is computed per ROLLUP grouping set
      When query
        """
        SELECT k, p, min_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY ROLLUP(k, p)
        ORDER BY k NULLS FIRST, p NULLS FIRST
        """
      Then query result ordered
        | k    | p    | r |
        | NULL | NULL | a |
        | k1   | NULL | a |
        | k1   | x    | a |
        | k1   | y    | b |
        | k2   | NULL | c |
        | k2   | x    | c |

    Scenario: min_by is computed per CUBE grouping set
      When query
        """
        SELECT k, p, min_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY CUBE(k, p)
        ORDER BY k NULLS FIRST, p NULLS FIRST
        """
      Then query result ordered
        | k    | p    | r |
        | NULL | NULL | a |
        | NULL | x    | a |
        | NULL | y    | b |
        | k1   | NULL | a |
        | k1   | x    | a |
        | k1   | y    | b |
        | k2   | NULL | c |
        | k2   | x    | c |

    @spark-4.2
    Scenario: min_by top-k is computed per ROLLUP grouping set
      When query
        """
        SELECT k, min_by(v, o, 2) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY ROLLUP(k)
        ORDER BY k NULLS FIRST
        """
      Then query result ordered
        | k    | r      |
        | NULL | [a, c] |
        | k1   | [a, b] |
        | k2   | [c, d] |

    Scenario: min_by can be used in HAVING
      When query
        """
        SELECT k, min_by(v, o) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        GROUP BY k
        HAVING min_by(v, o) <> 'a'
        ORDER BY k
        """
      Then query result ordered
        | k  | r |
        | k2 | c |

    Scenario: min_by in each branch of a UNION ALL
      When query
        """
        SELECT min_by(v, o) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k1'
        UNION ALL
        SELECT min_by(v, o) AS r FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p) WHERE k = 'k2'
        ORDER BY r
        """
      Then query result ordered
        | r |
        | a |
        | c |

    Scenario: min_by in a RANGE window frame
      When query
        """
        SELECT o, min_by(v, o) OVER (ORDER BY o RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS r
        FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p)
        ORDER BY o
        """
      Then query result ordered
        | o  | r |
        | 10 | a |
        | 20 | a |
        | 30 | c |
        | 50 | b |

  Rule: PIVOT

    Scenario: min_by as the PIVOT aggregate
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (min_by(v, o) FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query result ordered
        | k  | x | y    |
        | k1 | a | b    |
        | k2 | c | NULL |

    Scenario: min_by alongside another aggregate in a PIVOT
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (min_by(v, o) AS a, count(o) AS c FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query result ordered
        | k  | x_a | x_c | y_a  | y_c |
        | k1 | a   | 1   | b    | 1   |
        | k2 | c   | 2   | NULL | 0   |

    # On its general path Spark's PIVOT wraps every argument of the aggregate in
    # `IF(pivot_col <=> value, arg, NULL)` (`PivotTransformer`), `k` included, so `k` stops being
    # foldable and MaxMinByK rejects it. Sail pivots with an aggregate FILTER, keeps `k` a literal
    # and answers; reproducing the rejection is left for a follow-up.
    @sail-bug @spark-4.2
    Scenario: min_by top-k as the PIVOT aggregate
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (min_by(v, o, 2) FOR (p) IN ('x', 'y'))
        ORDER BY k
        """
      Then query error (?s)DATATYPE_MISMATCH.NON_FOLDABLE_INPUT.*foldable int expression

    # When every aggregate's result type is supported by `PivotFirst` (here INT), Spark takes that
    # path, which does not wrap the arguments, so the top-k call inside the aggregate is accepted.
    @spark-4.2
    Scenario: min_by top-k inside a PIVOT aggregate whose result type PivotFirst supports
      When query
        """
        SELECT * FROM (SELECT k, v, o, p FROM VALUES ('k1', 'a', 10, 'x'), ('k1', 'b', 50, 'y'), ('k2', 'c', 20, 'x'), ('k2', 'd', 30, 'x') AS t(k, v, o, p))
        PIVOT (size(min_by(v, o, 2)) FOR (p) IN ('x', 'y'))
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

    Scenario: min_by over a GROUP BY on shuffled input
      When query
        """
        SELECT g, min_by(x, y * 1000000 + id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r    |
        | 0 | 0    |
        | 1 | 6000 |
        | 2 | 5000 |
        | 3 | 4000 |
        | 4 | 3000 |
        | 5 | 2000 |
        | 6 | 1000 |

    Scenario: min_by with a DOUBLE key over a GROUP BY on shuffled input
      When query
        """
        SELECT g, min_by(x, d * 1000000 + id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r    |
        | 0 | 5054 |
        | 1 | 4054 |
        | 2 | 3054 |
        | 3 | 2054 |
        | 4 | 1054 |
        | 5 | 54   |
        | 6 | 6054 |

    Scenario: min_by with a STRUCT key that has NULL fields on shuffled input
      When query
        """
        SELECT min_by(x, named_struct('a', st.a, 'b', st.b)) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t
        """
      Then query result ordered
        | r |
        | 0 |

    @spark-4.2
    Scenario: min_by top-k merges partial states on shuffled input
      When query
        """
        SELECT g, min_by(x, y * 1000000 + id, 3) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r                    |
        | 0 | [0, 7000, 14000]     |
        | 1 | [6000, 13000, 6973]  |
        | 2 | [5000, 12000, 19000] |
        | 3 | [4000, 11000, 18000] |
        | 4 | [3000, 10000, 17000] |
        | 5 | [2000, 9000, 16000]  |
        | 6 | [1000, 8000, 15000]  |

    @spark-4.2
    Scenario: min_by top-k with a DOUBLE key on shuffled input
      When query
        """
        SELECT min_by(x, d * 1000000 + id, 5) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t
        """
      Then query result ordered
        | r                            |
        | [54, 1054, 2054, 3054, 4054] |

    @spark-4.2
    Scenario: min_by top-k with DISTINCT on shuffled input
      When query
        """
        SELECT g, min_by(DISTINCT CAST(y AS STRING), y, 3) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r         |
        | 0 | [0, 1, 2] |
        | 1 | [0, 1, 2] |
        | 2 | [0, 1, 2] |
        | 3 | [0, 1, 2] |
        | 4 | [0, 1, 2] |
        | 5 | [0, 1, 2] |
        | 6 | [0, 1, 2] |

    @spark-4.2
    Scenario: min_by top-k with FILTER on shuffled input
      When query
        """
        SELECT g, min_by(x, y * 1000000 + id, 2) FILTER (WHERE id % 3 = 0) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | r              |
        | 0 | [0, 14973]     |
        | 1 | [6000, 14946]  |
        | 2 | [12000, 5973]  |
        | 3 | [18000, 11973] |
        | 4 | [3000, 17973]  |
        | 5 | [9000, 2973]   |
        | 6 | [15000, 8973]  |

    Scenario: min_by in a sliding window frame on shuffled input
      When query
        """
        SELECT sum(CAST(r AS BIGINT) * id) AS h FROM (SELECT id, min_by(x, y * 1000000 + id) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h             |
        | 2664441313800 |

    @spark-4.2
    Scenario: min_by top-k in a sliding window frame on shuffled input
      When query
        """
        SELECT sum(CAST(r[0] AS BIGINT) * id + CAST(r[1] AS BIGINT)) AS h FROM (SELECT id, min_by(x, y * 1000000 + id, 2) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h             |
        | 2664792275124 |

    Scenario: min_by with a STRUCT key in a running window on shuffled input
      When query
        """
        SELECT sum(CAST(r AS BIGINT) * id) AS h FROM (SELECT id, min_by(x, st) OVER (PARTITION BY g ORDER BY id) AS r FROM (SELECT /*+ REPARTITION(8) */ id, id % 7 AS g, CAST(id AS STRING) AS x, (id * 37) % 1000 AS y, CAST((id * 37) % 1000 AS DOUBLE) * (CASE WHEN id % 2 = 0 THEN -1 ELSE 1 END) AS d, named_struct('a', CASE WHEN id % 5 = 0 THEN NULL ELSE (id * 13) % 100 END, 'b', id) AS st FROM range(0, 20000)) AS t)
        """
      Then query result ordered
        | h          |
        | 2999648589 |
