Feature: Function coercion and output schemas

  Scenario: Nested functions see coerced argument types
    When query
      """
      SELECT abs('-2') AS magnitude, array(1, CAST(2 AS BIGINT)) AS items
      """
    Then query result
      | magnitude | items  |
      | 2.0       | [1, 2] |

  Scenario: Array element nullability survives analysis
    When query
      """
      SELECT array(1, CAST(2 AS BIGINT)) AS items
      """
    Then query schema
      """
      root
       |-- items: array (nullable = false)
       |    |-- element: long (containsNull = false)
      """

  Scenario: Coercion traverses correlated scalar subqueries
    When query
      """
      SELECT id, (SELECT max(abs(CAST(b.id AS STRING)))
                  FROM VALUES (-2), (-1) AS b(id) WHERE b.id = t.id) AS magnitude
      FROM VALUES (-2), (-1) AS t(id)
      ORDER BY id
      """
    Then query result
      | id | magnitude |
      | -2 | 2.0       |
      | -1 | 1.0       |

  Scenario: Coercion traverses higher order function bodies
    When query
      """
      SELECT transform(array('-2', '-1'), x -> abs(x)) AS magnitudes
      """
    Then query result
      | magnitudes |
      | [2.0, 1.0] |

  Scenario: Coercion preserves repeated user facing names
    When query
      """
      WITH t AS (SELECT abs('-2') AS x, CAST(3 AS BIGINT) AS y)
      SELECT x AS value, y AS value FROM t
      """
    Then query result
      | value | value |
      | 2.0   | 3     |

  Scenario: Lambda parameters shadow grouping column substitutions
    When query
      """
      SELECT id, transform(array(7), id -> id + 1) AS items
      FROM range(2)
      GROUP BY id
      ORDER BY id
      """
    Then query result ordered
      | id | items |
      | 0  | [8]    |
      | 1  | [8]    |

  Scenario: Lambda captures bind to grouping outputs
    When query
      """
      SELECT id, transform(array(7), x -> x + id) AS items
      FROM range(2)
      GROUP BY id
      ORDER BY id
      """
    Then query result ordered
      | id | items |
      | 0  | [7]    |
      | 1  | [8]    |

  Scenario: Date comparisons coerce string bounds and list entries
    When query
      """
      SELECT d BETWEEN '2024-01-01' AND DATE '2024-01-31' AS in_range,
             d IN ('2024-01-02', '2024-02-01') AS in_list
      FROM VALUES (DATE '2024-01-02') t(d)
      """
    Then query result
      | in_range | in_list |
      | true     | true    |
