Feature: Join reorder falls back to greedy when emit threshold is exceeded

  Scenario: Five-table inner-join chain with emit_threshold=1 produces correct result via greedy fallback
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jret_a AS
      SELECT * FROM VALUES (1, 'a1'), (2, 'a2'), (3, 'a3') AS t(id, av)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jret_b AS
      SELECT * FROM VALUES (1, 'b1'), (2, 'b2'), (3, 'b3') AS t(id, bv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jret_c AS
      SELECT * FROM VALUES (1, 'c1'), (2, 'c2'), (3, 'c3') AS t(id, cv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jret_d AS
      SELECT * FROM VALUES (1, 'd1'), (2, 'd2'), (3, 'd3') AS t(id, dv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jret_e AS
      SELECT * FROM VALUES (1, 'e1'), (2, 'e2'), (3, 'e3') AS t(id, ev)
      """

    # With emit_threshold=1 (set via SAIL_OPTIMIZER__JOIN_REORDER__EMIT_THRESHOLD), the DPhyp
    # enumerator must trip the fallback path after the first emitted CSG-CMP pair and switch to
    # the greedy left-deep reconstruction. The result set must remain identical to the natural
    # inner join semantics.
    When query
      """
      SELECT a.id, a.av, b.bv, c.cv, d.dv, e.ev
      FROM jret_a a
      JOIN jret_b b ON a.id = b.id
      JOIN jret_c c ON a.id = c.id
      JOIN jret_d d ON a.id = d.id
      JOIN jret_e e ON a.id = e.id
      ORDER BY a.id
      """
    Then query result ordered
      | id | av | bv | cv | dv | ev |
      | 1  | a1 | b1 | c1 | d1 | e1 |
      | 2  | a2 | b2 | c2 | d2 | e2 |
      | 3  | a3 | b3 | c3 | d3 | e3 |
