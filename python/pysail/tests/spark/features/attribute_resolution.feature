Feature: Attribute identity in column resolution

  # TODO: Deduplicate matching roots by attribute identity. The identity tracker
  # preserves duplicate CTE outputs, but name resolution already rejected these
  # duplicate attributes before missing-input recovery was introduced.
  @sail-bug
  Scenario: Repeated CTE references allow duplicate occurrences of the same attribute
    When query
      """
      WITH t AS (SELECT id, id FROM range(2))
      SELECT b.id FROM t a CROSS JOIN t b
      """
    Then query result
      | id |
      | 0  |
      | 0  |
      | 1  |
      | 1  |
