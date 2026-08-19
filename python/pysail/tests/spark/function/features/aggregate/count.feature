Feature: count aggregate function

  Rule: count handles literal inputs

    Scenario: count distinguishes non-null, null, distinct, and filtered literals
      When query
        """
        SELECT
          COUNT(1) AS integer_count,
          COUNT('x') AS string_count,
          COUNT(NULL) AS null_count,
          COUNT(DISTINCT 1) AS distinct_count,
          COUNT(1) FILTER (WHERE id = 2) AS filtered_count
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | integer_count | string_count | null_count | distinct_count | filtered_count |
        | 3             | 3            | 0          | 1              | 1              |

    Scenario: count preserves typed null semantics
      When query
        """
        SELECT COUNT(CAST(NULL AS INT)) AS typed_null_count
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | typed_null_count |
        | 0                |

    Scenario: count literals over empty input return zero
      When query
        """
        SELECT COUNT(1) AS non_null_count, COUNT(NULL) AS null_count
        FROM (SELECT id FROM VALUES (1) AS t(id) WHERE false)
        """
      Then query result
        | non_null_count | null_count |
        | 0              | 0          |

    Scenario: count recognizes analyzed non-null expressions
      When query
        """
        SELECT
          COUNT(CAST(1 AS INT)) AS cast_count,
          COUNT(1 + 1) AS arithmetic_count,
          COUNT(COALESCE(NULL, 1)) AS coalesce_count,
          COUNT(named_struct('value', 1)) AS struct_count,
          COUNT(1, 2) AS multi_count
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | cast_count | arithmetic_count | coalesce_count | struct_count | multi_count |
        | 3          | 3                | 3              | 3            | 3           |

  Rule: count normalizes nullable argument lists without changing joint null semantics

    Scenario: count removes non-null arguments and duplicate direct columns
      When query
        """
        SELECT
          COUNT(a, 1) AS nullable_and_literal,
          COUNT(a, a) AS duplicate_column,
          COUNT(a, b) AS joint_count
        FROM VALUES (1, NULL), (NULL, 2), (3, 4) AS t(a, b)
        """
      Then query result
        | nullable_and_literal | duplicate_column | joint_count |
        | 2                    | 2                | 1           |

    Scenario: count preserves argument order and evaluation for different nullable expressions
      When query
        """
        SELECT COUNT(a, b) AS count_ab, COUNT(b, a) AS count_ba
        FROM VALUES (1, NULL), (NULL, 2), (3, 4) AS t(a, b)
        """
      Then query result
        | count_ab | count_ba |
        | 1        | 1        |

  Rule: count simplifies constant filters

    Scenario: true and false filters retain aggregate cardinality
      When query
        """
        SELECT
          COUNT(*) FILTER (WHERE TRUE) AS true_count,
          COUNT(*) FILTER (WHERE FALSE) AS false_count
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | true_count | false_count |
        | 3          | 0           |

  Rule: distinct literal counts depend on input cardinality

    Scenario: distinct literals over non-empty input
      When query
        """
        SELECT COUNT(DISTINCT 1) AS one_count, COUNT(DISTINCT NULL) AS null_count
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | one_count | null_count |
        | 1         | 0          |

    Scenario: distinct literals over empty input
      When query
        """
        SELECT COUNT(DISTINCT 1) AS one_count, COUNT(DISTINCT NULL) AS null_count
        FROM (SELECT id FROM VALUES (1) AS t(id) WHERE FALSE)
        """
      Then query result
        | one_count | null_count |
        | 0         | 0          |

  Rule: parameterless count is not count star

    Scenario: parameterless count is rejected by default
      When query
        """
        SELECT COUNT() FROM VALUES (1), (2) AS t(id)
        """
      Then query error (?i)count.*requires at least one parameter

    Scenario: legacy parameterless count returns zero
      Given config spark.sql.legacy.allowParameterlessCount = true
      When query
        """
        SELECT COUNT() AS count FROM VALUES (1), (2) AS t(id)
        """
      Then query result
        | count |
        | 0     |
