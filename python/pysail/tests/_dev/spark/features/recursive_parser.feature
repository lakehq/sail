Feature: Recursive SQL grammar

  Scenario: Nested queries and parenthesized joins
    When query
      """
      SELECT l.id, (SELECT max(id) FROM VALUES (3), (5) AS v(id)) AS max_id
      FROM VALUES (1), (2) AS l(id)
      JOIN (
        VALUES (1), (2) AS r(id)
        JOIN VALUES (2), (3) AS s(id) ON r.id = s.id
      ) ON l.id = r.id
      WHERE EXISTS (SELECT 1 FROM VALUES (2) AS t(id) WHERE t.id = l.id)
      """
    Then query result
      | id | max_id |
      | 2  | 5      |

  Scenario: Nested statements and recursive data types
    Given statement
      """
      EXPLAIN SELECT CAST(NULL AS ARRAY<STRUCT<x: ARRAY<INT>>>)
      """
    When query
      """
      SELECT typeof(CAST(NULL AS ARRAY<STRUCT<x: ARRAY<INT>>>)) AS type
      """
    Then query result
      | type                       |
      | array<struct<x:array<int>>> |
