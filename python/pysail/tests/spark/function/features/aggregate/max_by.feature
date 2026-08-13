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
