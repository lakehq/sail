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

  Rule: Plan snapshots — simplify removes UDF call only for single-argument identity cases (string/array/binary), and keeps it for coercion or multi-arg array concat

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
