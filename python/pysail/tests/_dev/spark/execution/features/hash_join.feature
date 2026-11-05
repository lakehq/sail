Feature: Hash joins in distributed execution

  Scenario Outline: Hash join preserves an input limit across shuffle
    When query
      """
      SELECT COUNT(*) AS total, COUNT(l.id) AS left_count, COUNT(r.id) AS right_count
      FROM (SELECT id FROM range(0, 16, 1, 4) LIMIT 3) l
      <join_type> JOIN range(0, 16, 1, 4) r ON l.id = r.id
      """
    Then query result collected
      | total   | left_count | right_count   |
      | <total> | 3          | <right_count> |

    Examples:
      | join_type | total | right_count |
      | LEFT      | 3     | 3           |
      | FULL      | 16    | 16          |

  Scenario Outline: NOT IN observes the entire probe side
    When query
      """
      SELECT COUNT(*) AS total, COUNT(id) AS non_null_count
      FROM (
        SELECT CASE WHEN id = 0 THEN CAST(NULL AS BIGINT) ELSE id END AS id
        FROM range(0, 16, 1, 4)
      ) l
      WHERE id NOT IN (
        SELECT <probe_key> FROM range(0, 64, 1, 4) WHERE <probe_filter>
      )
      """
    Then query result collected
      | total   | non_null_count   |
      | <total> | <non_null_count> |

    Examples:
      | probe_key                                      | probe_filter | total | non_null_count |
      | CASE WHEN id = 63 THEN NULL ELSE id + 1000 END   | id >= 0      | 0     | 0              |
      | id + 1000                                      | id = 63      | 15    | 15             |
      | id                                             | id >= 64     | 16    | 15             |
      | id % 8                                         | id >= 0      | 8     | 8              |

  Scenario: NOT IN preserves duplicate unmatched rows
    When query
      """
      SELECT id, COUNT(*) AS copies
      FROM (
        SELECT CASE WHEN id < 2 THEN CAST(NULL AS BIGINT) ELSE id DIV 2 END AS id
        FROM range(0, 8, 1, 4)
      ) l
      WHERE id NOT IN (SELECT id FROM range(0, 8, 1, 4) WHERE id = 1)
      GROUP BY id ORDER BY id
      """
    Then query result collected
      | id | copies |
      | 2  | 2      |
      | 3  | 2      |
