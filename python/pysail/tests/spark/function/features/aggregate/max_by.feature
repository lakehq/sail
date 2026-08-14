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

    Scenario: max_by rejects a MAP ordering column
      When query
        """
        SELECT max_by(v, o) AS result
        FROM VALUES ('lo', map('a', 1)), ('hi', map('b', 2)) AS t(v, o)
        """
      Then query error does not support ordering on type

    Scenario: max_by rejects a MAP ordering literal
      When query
        """
        SELECT max_by(1, map('a', 1)) AS result
        """
      Then query error does not support ordering on type

    Scenario: max_by rejects a MAP ordering column as a window function
      When query
        """
        SELECT max_by(v, map('a', i)) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(v, i)
        """
      Then query error does not support ordering on type

    Scenario: max_by rejects a VARIANT ordering column
      When query
        """
        SELECT max_by(x, parse_json(j)) AS result
        FROM VALUES ('a', '"aaa"'), ('b', '"b"') AS t(x, j)
        """
      Then query error does not support ordering on type

    Scenario: max_by rejects an ARRAY<MAP> ordering column
      When query
        """
        SELECT max_by(x, array(map('k', y))) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error does not support ordering on type

    # The check is recursive, so a MAP or VARIANT buried at any depth also fails.
    Scenario Outline: max_by rejects a nested <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(x, y)
        """
      Then query error does not support ordering on type

      Examples:
        | case               | ordering                   |
        | STRUCT<MAP>        | struct(map('k', y))        |
        | ARRAY<ARRAY<MAP>>  | array(array(map('k', y)))  |
        | STRUCT<ARRAY<MAP>> | struct(array(map('k', y))) |
        | ARRAY<VARIANT>     | array(parse_json(x))       |
        | STRUCT<VARIANT>    | struct(parse_json(x))      |

    # Spark's CalendarIntervalType is not an AtomicType, so it is not orderable,
    # unlike the ANSI day-time and year-month interval types below.
    Scenario: max_by rejects a calendar INTERVAL ordering column
      When query
        """
        SELECT max_by(x, make_interval(0, 0, 0, y)) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query error does not support ordering on type

    # The rejections above only discriminate if the accepted types are pinned too: a check that
    # rejected every type would satisfy every scenario in this Rule. These are the orderable
    # counterparts, including the ANSI intervals the calendar one above is contrasted with.
    Scenario Outline: max_by accepts an orderable <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('lo', 1), ('hi', 2) AS t(x, y)
        """
      Then query result
        | result |
        | hi     |

      Examples:
        | case          | ordering                                                       |
        | STRUCT<INT>   | struct(y)                                                      |
        | ARRAY<INT>    | array(y)                                                       |
        | INTERVAL DAY  | make_dt_interval(0, 0, 0, y)                                   |
        | INTERVAL YEAR | make_ym_interval(0, y)                                         |
        | BINARY        | CAST(CAST(y AS STRING) AS BINARY)                              |
        | DATE          | date_add(DATE '2024-01-01', y)                                 |
        | TIMESTAMP     | TIMESTAMP '2024-01-01 00:00:00' + make_dt_interval(0, 0, 0, y) |

    # VOID is orderable in Spark: OrderUtils.isOrderable matches `case NullType => true` before
    # the AtomicType case. So the call is accepted, and every row is then skipped for having a
    # NULL ordering value, which is what makes the result NULL rather than an error.
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
    # case, because they are opaque WKB bytes with no meaningful ordering. Sail cannot see this
    # from `coerce_types`: it lowers both to plain BINARY and keeps the geo identity in the
    # field metadata, which the hook never receives.
    @sail-bug @spark-4.2
    Scenario: max_by rejects a GEOMETRY ordering column
      When query
        """
        SELECT max_by(v, st_geomfromwkb(w)) AS result
        FROM VALUES ('a', X'0101000000000000000000F03F0000000000000040'),
                    ('b', X'010100000000000000000000400000000000000040') AS t(v, w)
        """
      Then query error does not support ordering on type

  Rule: Clause surface

    # Spark decides this in FunctionResolution.validateFunction: max_by is a plain
    # AggregateFunction, so DISTINCT, FILTER and OVER are accepted, while IGNORE NULLS
    # (applyIgnoreNulls has a whitelist max_by is not in) and WITHIN GROUP (it is not
    # SupportsOrderingWithinGroup) are rejected at analysis. Sail's resolver forwards all
    # four clauses to the aggregate with no gate at all, so the rejected two are silently
    # accepted AND change the answer. That is a sail-plan gap, not a max_by one.

    Scenario Outline: max_by accepts <case>
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
    Scenario: max_by accepts OVER
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

    @sail-bug
    Scenario: max_by rejects IGNORE NULLS
      When query
        """
        SELECT max_by(x, y) IGNORE NULLS AS result
        FROM VALUES (CAST(NULL AS STRING), 50), ('b', 10) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support IGNORE NULLS

    @sail-bug
    Scenario: max_by rejects WITHIN GROUP
      When query
        """
        SELECT max_by(x, y) WITHIN GROUP (ORDER BY x) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query error INVALID_SQL_SYNTAX.*does not support WITHIN GROUP

    @sail-bug
    Scenario: max_by rejects DISTINCT combined with OVER
      When query
        """
        SELECT max_by(DISTINCT x, y) OVER (PARTITION BY 1) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20), ('a', 10) AS t(x, y)
        """
      Then query error DISTINCT_WINDOW_FUNCTION_UNSUPPORTED

    # The mirror image: Spark allows FILTER on a window aggregate, Sail's window match arm
    # requires `filter: None` and rejects it.
    @sail-bug
    Scenario: max_by supports FILTER combined with OVER
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

  Rule: Arity

    # Spark's MaxMinBy is BinaryLike. Sail used to panic here and kill the RPC, because
    # Signature::user_defined skips DataFusion's arity gate and coerce_types is the only one.
    Scenario Outline: max_by rejects a call with <case>
      When query
        """
        SELECT max_by(<args>)
        """
      Then query error (?i)requires

      Examples:
        | case            | args       |
        | no arguments    |            |
        | one argument    | 1          |
        | four arguments  | 1, 2, 3, 4 |

    # Spark 4.2 added the top-k form, which returns an array of the k values
    # (MaxMinByK.scala). Sail does not implement it; it now rejects the third argument
    # instead of silently dropping it and returning a scalar.
    @sail-bug @spark-4.2
    Scenario: max_by supports the three-argument top-k form
      When query
        """
        SELECT max_by(x, y, 2) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result   |
        | [b, c]   |

  Rule: Orderable ordering types are accepted

    Scenario Outline: max_by accepts a <case> ordering column
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query result
        | result |
        | b      |

      Examples:
        | case                | ordering                   |
        | STRUCT              | struct(y, x)               |
        | ARRAY<INT>          | array(y)                   |
        | ARRAY<STRUCT>       | array(struct(y))           |
        | STRUCT<ARRAY<INT>>  | struct(array(y))           |
        | STRUCT<STRUCT>      | struct(struct(y))          |
        | BINARY              | CAST(x AS BINARY)          |
        | DECIMAL             | CAST(y AS DECIMAL(10,2))   |
        | day-time INTERVAL   | INTERVAL '1' DAY * y       |
        | year-month INTERVAL | make_ym_interval(y)        |

    # Same orderable year-month type as above, but built by multiplication:
    # Sail cannot type `Interval(YearMonth) * Int32` and fails before it ever
    # gets to the orderability check.
    @sail-bug
    Scenario: max_by accepts a multiplied year-month INTERVAL ordering column
      When query
        """
        SELECT max_by(x, INTERVAL '1' YEAR * y) AS result
        FROM VALUES ('a', 1), ('b', 2) AS t(x, y)
        """
      Then query result
        | result |
        | b      |

    # A constant ordering key makes every row tie. Spark keeps the newer row on a tie
    # (MaxByAndMinBy.scala: `If(old > new, old, new)` is false when they are equal), so the
    # answer is the last one. Foldable arguments are constant-folded to the same literal.
    Scenario Outline: max_by accepts the constant ordering key <case>
      When query
        """
        SELECT max_by(x, <ordering>) AS result
        FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS t(x, y)
        """
      Then query result
        | result |
        | c      |

      Examples:
        | case            | ordering         |
        | literal         | 1                |
        | foldable sum    | 1 + 1            |
        | foldable concat | concat('a', 'b') |

    # NullType is orderable in Spark, which returns NULL here.
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
    Scenario Outline: max_by accepts a <case> value argument
      When query
        """
        SELECT <value> AS result
        FROM VALUES ('{"v":1}', 1), ('{"v":2}', 2) AS t(j, y)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | value                                      | result  |
        | MAP         | max_by(map('a', y), y)['a']                | 2       |
        | VARIANT     | to_json(max_by(parse_json(j), y))          | {"v":2} |
        | ARRAY<MAP>  | max_by(array(map('k', y)), y)[0]['k']      | 2       |
        | STRUCT<MAP> | max_by(struct(map('k', y) AS m), y).m['k'] | 2       |
