@concat_ws_simplify
Feature: concat_ws() — simplify hook (null folding and null-arg pruning)

  Rule: A null separator literal folds the whole call to null

    Scenario: null separator literal with scalar values is null
      When query
        """
        SELECT concat_ws(NULL, 'a', 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

    # The fold short-circuits even non-literal value arguments: a null separator
    # makes every row null without the kernel touching the value column.
    Scenario: null separator literal over a column is null per row
      When query
        """
        SELECT concat_ws(NULL, v) AS result FROM VALUES
          ('x'),
          ('y')
        AS t(v) ORDER BY v
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: Null value literals are pruned (Spark skips them)

    Scenario: null value literal between scalars is dropped
      When query
        """
        SELECT concat_ws(',', 'a', NULL, 'b') AS result
        """
      Then query result
        | result |
        | a,b    |

    # Mixed column + null literal: not all-literal, so constant folding cannot
    # touch it — the simplify hook prunes the trailing NULL and the kernel runs
    # on `(',', v)` alone.
    Scenario: null value literal trailing a column is dropped
      When query
        """
        SELECT concat_ws(',', v, NULL) AS result FROM VALUES
          ('x'),
          ('y'),
          (CAST(NULL AS STRING))
        AS t(v) ORDER BY v NULLS FIRST
        """
      Then query result ordered
        | result |
        |        |
        | x      |
        | y      |

  Rule: A separator-only literal call folds to the empty string

    Scenario: separator-only literal is the empty string
      When query
        """
        SELECT concat_ws('-') AS result
        """
      Then query result
        | result |
        |        |

  Rule: Extreme edge cases — null folding interacts with pruning and rule ordering

    # Separator-only NULL must fold to NULL (the null-separator rule), NOT to the
    # empty string (the separator-only rule). Rule ordering matters here.
    Scenario: null separator with no value arguments is null, not empty string
      When query
        """
        SELECT concat_ws(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    # A numeric (non-string) null separator literal still folds the whole call.
    Scenario: numeric null separator folds to null
      When query
        """
        SELECT concat_ws(CAST(NULL AS INT), 'a', 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

    # Every value argument is a null literal: after pruning, only the separator
    # literal remains, which folds to the empty string (not null).
    Scenario: all value arguments null leaves the empty string
      When query
        """
        SELECT concat_ws(',', NULL, NULL, NULL) AS result
        """
      Then query result
        | result |
        |        |

    # Null literals are pruned but a non-literal empty array is kept and rendered
    # as nothing by the kernel — still the empty string.
    Scenario: null literals pruned around an empty array
      When query
        """
        SELECT concat_ws(',', NULL, array(), NULL) AS result
        """
      Then query result
        | result |
        |        |

    # Nested: the inner null-separator call folds to a null literal, which the
    # outer call then prunes as a null value argument.
    Scenario: inner null-separator fold is pruned by the outer call
      When query
        """
        SELECT concat_ws('|', concat_ws(NULL, 'a'), 'b') AS result
        """
      Then query result
        | result |
        | b      |

    # Pruning a trailing null literal must not disturb the kernel's per-row null
    # handling for a column separator that itself has null rows.
    Scenario: trailing null literal pruned with a column separator that has null rows
      When query
        """
        SELECT concat_ws(sep, a, NULL, b) AS result FROM VALUES
          (0, ',', 'a', 'b'),
          (1, CAST(NULL AS STRING), 'x', 'y')
        AS t(id, sep, a, b) ORDER BY id
        """
      Then query result ordered
        | result |
        | a,b    |
        | NULL   |

  Rule: Nullability — the result is null only when the separator is null

    Scenario: a non-null literal separator yields a non-nullable result
      When query
        """
        SELECT concat_ws(',', a, b) AS result FROM VALUES
          ('x', 'y')
        AS t(a, b)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: nullable value arguments do not make the result nullable
      When query
        """
        SELECT concat_ws(',', v) AS result FROM VALUES
          ('x'),
          (CAST(NULL AS STRING))
        AS t(v)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable separator yields a nullable result
      When query
        """
        SELECT concat_ws(sep, 'a') AS result FROM VALUES
          ('x'),
          (CAST(NULL AS STRING))
        AS t(sep)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Plan snapshots — simplify folds null and prunes null args, but keeps the kernel where it is still needed

    @sail-only
    Scenario: EXPLAIN null separator literal over a column — no spark_concat_ws in plan
      When query
        """
        EXPLAIN SELECT concat_ws(NULL, v) AS result FROM VALUES
          ('x'),
          ('y')
        AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN null value literal trailing a column — spark_concat_ws stays, NULL pruned
      When query
        """
        EXPLAIN SELECT concat_ws(',', v, NULL) AS result FROM VALUES
          ('x'),
          ('y')
        AS t(v)
        """
      Then query plan matches snapshot

    # The inner null-separator call short-circuits the column and folds to null;
    # the outer call prunes it, leaving a fully-constant result — no
    # spark_concat_ws survives despite the column reference.
    @sail-only
    Scenario: EXPLAIN nested null-separator fold over a column collapses to a constant
      When query
        """
        EXPLAIN SELECT concat_ws('|', concat_ws(NULL, v), 'b') AS result FROM VALUES
          ('x'),
          ('y')
        AS t(v)
        """
      Then query plan matches snapshot

    # Dead-code elimination: under a null separator, an expensive value argument
    # (here spark_abs) is dropped from the plan entirely and never evaluated.
    @sail-only
    Scenario: EXPLAIN null separator drops an expensive value argument
      When query
        """
        EXPLAIN SELECT concat_ws(NULL, abs(v), CAST(v AS STRING)) AS result FROM VALUES
          (-5),
          (3)
        AS t(v)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN column-only separator — spark_concat_ws stays in plan
      When query
        """
        EXPLAIN SELECT concat_ws(v) AS result FROM VALUES
          ('x'),
          ('y')
        AS t(v)
        """
      Then query plan matches snapshot
