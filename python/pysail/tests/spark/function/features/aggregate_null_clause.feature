Feature: aggregate NULL clauses

  Rule: Unsupported aggregate NULL handling

    Scenario: unsupported aggregate rejects IGNORE NULLS
      When query
      """
      SELECT count(x) IGNORE NULLS AS result
      FROM (VALUES (1), (NULL)) AS t(x)
      """
      Then query error IGNORE NULLS

    Scenario: unsupported aggregate accepts RESPECT NULLS as the default
      When query
      """
      SELECT count(x) RESPECT NULLS AS result
      FROM (VALUES (1), (NULL), (2)) AS t(x)
      """
      Then query result
      | result |
      | 2      |
