Feature: Attribute identity in column resolution

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

  Scenario: Derived tables allow duplicate occurrences of the same attribute
    When query
      """
      SELECT id FROM (SELECT id, id FROM range(2))
      """
    Then query result
      | id |
      | 0  |
      | 1  |

  Scenario: Duplicate occurrences of a union attribute resolve to the first column
    When query
      """
      SELECT id FROM (SELECT id, id FROM range(2) UNION ALL SELECT id + 10, id + 20 FROM range(2))
      WHERE id > 5
      """
    Then query result
      | id |
      | 10 |
      | 11 |

  @spark-4.2
  @sail-bug
  Scenario: Extracting a field or item from a NULL column yields NULL
    When query
      """
      SELECT n.x, n[0] FROM (SELECT NULL AS n)
      """
    Then query result
      | x    | n[0] |
      | NULL | NULL |
