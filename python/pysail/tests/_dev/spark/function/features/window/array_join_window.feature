Feature: array_join as window function

  Rule: array_join with window aggregation (expanding windows)
    Scenario: array_join over window with partition using expanding frame
      When query
        """
        SELECT
          pk,
          id,
          s,
          ARRAY_JOIN(COLLECT_LIST(s) OVER (PARTITION BY pk ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), ',') AS joined
        FROM (
          SELECT 1 AS pk, 1 AS id, 'a' AS s
          UNION ALL SELECT 1, 2, 'b'
          UNION ALL SELECT 2, 3, 'c'
        )
        ORDER BY id
        """
      Then query result ordered
        | pk | id | s | joined |
        | 1  | 1  | a | a      |
        | 1  | 2  | b | a,b    |
        | 2  | 3  | c | c      |

    Scenario: array_join over simple window
      When query
        """
        SELECT
          x,
          ARRAY_JOIN(COLLECT_LIST(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), ',') AS joined
        FROM (SELECT explode(array('a', 'b', 'c')) AS x)
        """
      Then query result ordered
        | x | joined |
        | a | a      |
        | b | a,b    |
        | c | a,b,c  |
