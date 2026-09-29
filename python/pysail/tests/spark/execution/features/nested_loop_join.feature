Feature: Nested loop joins in distributed execution

  Scenario Outline: Row existence combines all probe partitions
    When query
      """
      SELECT COUNT(*) AS count FROM (
        SELECT 1 AS present
        WHERE <predicate> (
          SELECT id FROM range(0, 4, 1, 4) WHERE id >= <minimum>
        )
      )
      """
    Then query result collected
      | count   |
      | <count> |

    Examples:
      | predicate  | minimum | count |
      | EXISTS     | 3       | 1     |
      | EXISTS     | 4       | 0     |
      | NOT EXISTS | 3       | 0     |
      | NOT EXISTS | 4       | 1     |
