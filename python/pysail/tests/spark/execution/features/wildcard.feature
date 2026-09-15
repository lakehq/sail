Feature: Wildcard resolution

  @sail-only
  Scenario: Unqualified wildcard resolves the outer query without an inner input
    When query
      """
      SELECT q.* FROM VALUES (1), (2) AS t(id), LATERAL (SELECT *) AS q ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Qualified wildcard resolves the outer query without an inner input
    When query
      """
      SELECT q.* FROM VALUES (1), (2) AS t(id), LATERAL (SELECT t.*) AS q ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Qualified outer wildcard resolves with an inner input
    When query
      """
      SELECT q.* FROM VALUES (1), (2) AS t(id),
        LATERAL (SELECT t.* FROM VALUES (9) AS u(id)) AS q
      ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
    When query
      """
      EXPLAIN CODEGEN
      SELECT q.* FROM VALUES (1), (2) AS t(id),
        LATERAL (SELECT t.* FROM VALUES (9) AS u(id)) AS q
      ORDER BY id
      """
    Then query plan matches snapshot

  Scenario: Wildcard preserves projection order alongside a generator
    When query
      """
      SELECT 0 AS first, *, explode(array(7, 8)) AS last
      FROM VALUES (1, 'x') AS t(id, payload)
      ORDER BY last
      """
    Then query result ordered
      | first | id | payload | last |
      | 0     | 1  | x       | 7    |
      | 0     | 1  | x       | 8    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT 0 AS first, *, explode(array(7, 8)) AS last
      FROM VALUES (1, 'x') AS t(id, payload)
      ORDER BY last
      """
    Then query plan matches snapshot

  Scenario: Qualified wildcard selects one side of a join
    When query
      """
      SELECT l.*, r.payload AS right_payload
      FROM VALUES (1, 'a') AS l(id, payload)
      JOIN VALUES (1, 'b') AS r(id, payload) ON l.id = r.id
      """
    Then query result
      | id | payload | right_payload |
      | 1  | a       | b             |

  Scenario: Nested struct wildcard preserves field names
    When query
      """
      SELECT rec.nested.*
      FROM (SELECT named_struct('nested', named_struct('a', 1, 'b', 'x')) AS rec)
      """
    Then query result
      | a | b |
      | 1 | x |

  Scenario: Wildcard excludes hidden keys of a full using join
    When query
      """
      SELECT * FROM VALUES (1), (2) AS l(id)
      FULL JOIN VALUES (2), (3) AS r(id) USING (id)
      ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
      | 3  |

  Scenario: Count wildcards resolve to row counting and visible distinct keys
    When query
      """
      SELECT COUNT(*) AS rows, COUNT(DISTINCT *) AS distinct_rows
      FROM VALUES (1), (2) AS l(id)
      FULL JOIN VALUES (2), (3) AS r(id) USING (id)
      """
    Then query result
      | rows | distinct_rows |
      | 3    | 3             |
    When query
      """
      EXPLAIN CODEGEN
      SELECT COUNT(*) AS rows, COUNT(DISTINCT *) AS distinct_rows
      FROM VALUES (1), (2) AS l(id)
      FULL JOIN VALUES (2), (3) AS r(id) USING (id)
      """
    Then query plan matches snapshot

  Scenario: Struct and window count wildcards resolve to concrete arguments
    When query
      """
      SELECT struct(*) AS record, COUNT(*) OVER (PARTITION BY g) AS rows
      FROM VALUES ('x', NULL), ('x', 1), ('y', NULL) AS t(g, value)
      """
    Then query result
      | record    | rows |
      | {x, NULL} | 2    |
      | {x, 1}    | 2    |
      | {y, NULL} | 1    |
    When query
      """
      EXPLAIN CODEGEN
      SELECT struct(*) AS record, COUNT(*) OVER (PARTITION BY g) AS rows
      FROM VALUES ('x', NULL), ('x', 1), ('y', NULL) AS t(g, value)
      """
    Then query plan matches snapshot
