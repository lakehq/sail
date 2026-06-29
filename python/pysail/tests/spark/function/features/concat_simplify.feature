@concat_simplify
Feature: concat() — simplify hook (single-argument identity)

  Rule: String, array, and binary inputs — identity, no coercion needed

    Scenario: concat of single string literal is identity
      When query
        """
        SELECT concat('hello') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: concat of single string column propagates values and nulls
      When query
        """
        SELECT concat(v) AS result FROM VALUES
          ('hello'),
          ('world'),
          (NULL)
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result |
        | NULL   |
        | hello  |
        | world  |

    Scenario: concat of single array column is identity
      When query
        """
        SELECT concat(v) AS result FROM VALUES
          (array(1, 2, 3)),
          (array(4, 5))
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result    |
        | [1, 2, 3] |
        | [4, 5]    |

    Scenario: concat of single null string returns null
      When query
        """
        SELECT concat(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    # Regression coverage: `concat(array())` has arg type `List(Null)` which the
    # simplify hook matches. `return_type` for `[List(Null)]` is also `List(Null)`,
    # so simplify is type-preserving here — both the simplify and invoke paths
    # produce an empty list.
    Scenario: concat of single empty array literal returns empty array
      When query
        """
        SELECT concat(array()) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: concat of single binary column is identity
      When query
        """
        SELECT concat(v) AS result FROM VALUES
          (CAST('hello' AS BINARY)),
          (CAST('world' AS BINARY)),
          (CAST(NULL AS BINARY))
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result            |
        | NULL              |
        | [68 65 6C 6C 6F]  |
        | [77 6F 72 6C 64]  |

  Rule: Non-string inputs — coerced to string, not simplified away

    Scenario: concat of integer literal coerces to string
      When query
        """
        SELECT concat(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: concat of boolean literal coerces to string
      When query
        """
        SELECT concat(true) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: concat of integer column coerces to string and propagates nulls
      When query
        """
        SELECT concat(v) AS result FROM VALUES
          (CAST(1 AS INT)),
          (CAST(2 AS INT)),
          (CAST(NULL AS INT))
        AS t(v) ORDER BY result
        """
      Then query result ordered
        | result |
        | NULL   |
        | 1      |
        | 2      |

    # Regression coverage: timestamps must NOT be matched by the simplify hook
    # because `invoke_with_args` applies Spark-specific timestamp formatting
    # (`spark_format_timestamp_str`) that returns `YYYY-MM-DD HH:MM:SS` without
    # the Arrow `T` separator or timezone suffix. Simplifying `concat(ts)` to
    # `ts` would leak Arrow's ISO 8601 rendering instead.
    Scenario: concat of single timestamp coerces to Spark-formatted string
      When query
        """
        SELECT concat(CAST('2024-01-15 12:00:00' AS TIMESTAMP)) AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: concat of a non-midnight timestamp strips the Arrow T separator
      When query
        """
        SELECT concat(CAST('2024-12-31 23:59:59' AS TIMESTAMP)) AS result
        """
      Then query result
        | result              |
        | 2024-12-31 23:59:59 |

    Scenario: concat of a string and a timestamp formats the timestamp in place
      When query
        """
        SELECT concat('ts=', CAST('2024-01-15 12:30:45' AS TIMESTAMP)) AS result
        """
      Then query result
        | result                 |
        | ts=2024-01-15 12:30:45 |

  Rule: Null propagation — any NULL argument makes the whole concat NULL

    Scenario: string concat with a NULL literal is null
      When query
        """
        SELECT concat('a', NULL, 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: string concat with a typed NULL is null
      When query
        """
        SELECT concat('a', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: binary concat with a NULL is null
      When query
        """
        SELECT concat(X'4869', CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array concat with a typed NULL array is null
      When query
        """
        SELECT concat(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    # Folding to NULL must NOT mask the type error: an array concatenated with an
    # untyped NULL is still invalid, exactly as in Spark.
    Scenario: array concat with an untyped NULL still errors
      When query
        """
        SELECT concat(array(1, 2), NULL) AS result
        """
      Then query error .*

    # Null propagation over a column: the NULL literal forces every row to NULL.
    Scenario: string concat with a NULL literal over a column is null per row
      When query
        """
        SELECT concat(v, NULL) AS result FROM VALUES
          ('a'),
          ('b')
        AS t(v) ORDER BY v
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: CombineConcats — nested concat flattens (semantics preserved)

    Scenario: left-nested string concat
      When query
        """
        SELECT concat(concat('a', 'b'), 'c') AS result
        """
      Then query result
        | result |
        | abc    |

    Scenario: both-sides-nested string concat
      When query
        """
        SELECT concat(concat('a', 'b'), concat('c', 'd')) AS result
        """
      Then query result
        | result |
        | abcd   |

    Scenario: deeply nested string concat
      When query
        """
        SELECT concat(concat(concat('a', 'b'), 'c'), 'd') AS result
        """
      Then query result
        | result |
        | abcd   |

    Scenario: NULL bubbled up from an inner concat propagates
      When query
        """
        SELECT concat(concat('a', NULL), 'c') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: nested array concat flattens
      When query
        """
        SELECT concat(concat(array(1, 2), array(3)), array(4)) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: nested array concat with a NULL array propagates
      When query
        """
        SELECT concat(concat(array(1), CAST(NULL AS ARRAY<INT>)), array(2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: nested binary concat flattens
      When query
        """
        SELECT hex(concat(concat(X'01', X'02'), X'03')) AS result
        """
      Then query result
        | result |
        | 010203 |

    Scenario: nested concat with integer coercion flattens
      When query
        """
        SELECT concat(concat('a', 1), 'b') AS result
        """
      Then query result
        | result |
        | a1b    |

  Rule: CombineConcats composes with null propagation when a value arg is an expression

    Scenario: nested concat keeps an abs() value argument (last position)
      When query
        """
        SELECT concat(concat(a, b), CAST(abs(x) AS STRING)) AS result FROM VALUES
          ('a', 'b', -5),
          ('c', 'd', 3)
        AS t(a, b, x) ORDER BY a
        """
      Then query result ordered
        | result |
        | ab5    |
        | cd3    |

    Scenario: nested concat keeps an abs() value argument (inner position)
      When query
        """
        SELECT concat(concat(a, CAST(abs(x) AS STRING)), b) AS result FROM VALUES
          ('a', 'b', -5),
          ('c', 'd', 3)
        AS t(a, b, x) ORDER BY a
        """
      Then query result ordered
        | result |
        | a5b    |
        | c3d    |

    Scenario: an inner NULL forces NULL and drops the abs() argument
      When query
        """
        SELECT concat(concat(a, NULL), CAST(abs(x) AS STRING)) AS result FROM VALUES
          ('a', 'b', -5),
          ('c', 'd', 3)
        AS t(a, b, x) ORDER BY a
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: Nullability — the result is null iff any argument is null

    # Non-null literal arguments (Sail marks VALUES-derived columns nullable even
    # without a NULL row, unlike Spark — orthogonal to concat, so use literals).
    Scenario: all non-null arguments yield a non-nullable result
      When query
        """
        SELECT concat('a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable argument yields a nullable result
      When query
        """
        SELECT concat(a, b) AS result FROM VALUES
          ('x', 'y'),
          (CAST(NULL AS STRING), 'z')
        AS t(a, b)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: All-literal calls are constant-folded by DataFusion (not the simplify hook)

    # `concat` is Immutable, so an all-literal call is evaluated at planning time by
    # DataFusion's generic `ConstEvaluator` — independent of `SparkConcat::simplify`.
    # Documented here as a regression guard in case that generic folding changes.
    Scenario: all-literal concat folds to a single string
      When query
        """
        SELECT concat('abc', 'def') AS result
        """
      Then query result
        | result |
        | abcdef |

    Scenario: all-literal concat of three strings folds
      When query
        """
        SELECT concat('abc', 'def', 'ghi') AS result
        """
      Then query result
        | result    |
        | abcdefghi |

  Rule: Plan snapshots — simplify removes UDF call only for single-argument identity cases (string/array/binary), and keeps it for coercion or multi-arg array concat

    # ConstEvaluator (generic, not the simplify hook) folds an all-literal concat to
    # the resulting string literal — no spark_concat survives in the plan.
    @sail-only
    Scenario: EXPLAIN all-literal concat has no spark_concat in the plan
      When query
        """
        EXPLAIN SELECT concat('abc', 'def') AS result
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN concat of single string column — no spark_concat in plan
      When query
        """
        EXPLAIN SELECT concat(v) AS result FROM VALUES
          ('hello'),
          ('world')
        AS t(v)
        """
      Then query plan matches snapshot

    # CombineConcats: nested concat over columns collapses to a single flat
    # spark_concat call in the plan (one kernel pass instead of two).
    @sail-only
    Scenario: EXPLAIN nested concat over columns flattens to one spark_concat
      When query
        """
        EXPLAIN SELECT concat(concat(a, b), c) AS result FROM VALUES
          ('x', 'y', 'z'),
          ('p', 'q', 'r')
        AS t(a, b, c)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN deeply nested concat over columns flattens to one spark_concat
      When query
        """
        EXPLAIN SELECT concat(concat(concat(a, b), c), d) AS result FROM VALUES
          ('w', 'x', 'y', 'z')
        AS t(a, b, c, d)
        """
      Then query plan matches snapshot

    # CombineConcats keeps a real value argument: the nested concat flattens and
    # the abs() is spliced in (still evaluated) — one flat spark_concat call.
    @sail-only
    Scenario: EXPLAIN nested concat flattens and keeps the abs argument
      When query
        """
        EXPLAIN SELECT concat(concat(a, b), CAST(abs(x) AS STRING)) AS result FROM VALUES
          ('a', 'b', -5),
          ('c', 'd', 3)
        AS t(a, b, x)
        """
      Then query plan matches snapshot

    # Composition with null propagation: an inner NULL folds the whole call to
    # NULL, so even the abs() inside the flattened call is dropped (never run).
    @sail-only
    Scenario: EXPLAIN inner NULL folds the flattened concat and drops abs
      When query
        """
        EXPLAIN SELECT concat(concat(a, NULL), CAST(abs(x) AS STRING)) AS result FROM VALUES
          ('a', 'b', -5),
          ('c', 'd', 3)
        AS t(a, b, x)
        """
      Then query plan matches snapshot

    # Dead-code elimination: a NULL literal argument folds the whole concat to
    # NULL, so an expensive sibling argument (here spark_abs) is dropped from the
    # plan entirely and never evaluated.
    @sail-only
    Scenario: EXPLAIN concat with a NULL literal drops an expensive argument
      When query
        """
        EXPLAIN SELECT concat(CAST(abs(v) AS STRING), CAST(NULL AS STRING)) AS result FROM VALUES
          (-5),
          (3)
        AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN concat of two array columns — spark_concat stays in plan
      When query
        """
        EXPLAIN SELECT concat(a, b) AS result FROM VALUES
          (array(1, 2), array(3, 4)),
          (array(5), array(6, 7))
        AS t(a, b)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN concat of single integer column — spark_concat stays in plan
      When query
        """
        EXPLAIN SELECT concat(v) AS result FROM VALUES
          (CAST(1 AS INT)),
          (CAST(2 AS INT))
        AS t(v)
        """
      Then query plan matches snapshot
