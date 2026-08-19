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
