@typeof
Feature: typeof reports the analyzed type without changing the query shape

  # `typeof` only needs its argument's TYPE, so it folds to a string literal during
  # analysis. That must not change what the query returns: wrapping an aggregate in
  # `typeof` still leaves the query aggregated. All values validated against Spark
  # JVM 4.1.1.

  Rule: typeof over an aggregate keeps the implicit global aggregation

    @sail-bug
    Scenario: typeof of a sum over two rows returns one row
      # Sail folds `typeof` to a literal and drops the aggregate with it, so the
      # projection runs per input row and the query returns TWO rows.
      When query
        """
        SELECT typeof(sum(v)) AS t FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | t      |
        | bigint |

    @sail-bug
    Scenario: typeof of a count over two rows returns one row
      When query
        """
        SELECT typeof(count(v)) AS t FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | t      |
        | bigint |

    Scenario: typeof of a sum with GROUP BY returns one row per group
      # The explicit grouping keeps the aggregate node, so this case already agrees.
      When query
        """
        SELECT typeof(sum(v)) AS t FROM VALUES (1, 'a'), (2, 'a') AS t(v, g) GROUP BY g
        """
      Then query result
        | t      |
        | bigint |

    Scenario: the aggregate itself is unaffected
      When query
        """
        SELECT sum(v) AS r FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | r |
        | 3 |
