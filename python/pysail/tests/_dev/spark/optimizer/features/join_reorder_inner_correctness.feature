Feature: Join reorder preserves inner-join correctness contracts

  Scenario: Inner join reorder preserves duplicate names and null-safe equality
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_a AS
      SELECT * FROM VALUES
        (CAST(1 AS INT), 'a'),
        (CAST(NULL AS INT), 'na')
      AS t(id, av)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_b AS
      SELECT * FROM VALUES
        (CAST(1 AS INT), 'b'),
        (CAST(2 AS INT), 'b2'),
        (CAST(NULL AS INT), 'nb')
      AS t(id, bv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_c AS
      SELECT * FROM VALUES
        (CAST(1 AS INT), 'c'),
        (CAST(NULL AS INT), 'nc')
      AS t(id, cv)
      """

    When query
      """
      SELECT
        a.id AS a_id,
        b.id AS b_id,
        c.id AS c_id,
        a.av,
        b.bv,
        c.cv
      FROM jric_a a
      JOIN jric_b b ON a.id <=> b.id
      JOIN jric_c c ON b.id <=> c.id
      ORDER BY a.av, b.bv, c.cv
      """
    Then query result ordered
      | a_id | b_id | c_id | av | bv | cv |
      | 1    | 1    | 1    | a  | b  | c  |
      | NULL | NULL | NULL | na | nb | nc |

  Scenario: Non-inner joins remain boundaries while inner joins around them are optimized
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_l AS
      SELECT * FROM VALUES
        (1, 'l1'),
        (2, 'l2')
      AS t(id, lv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_r AS
      SELECT * FROM VALUES
        (1, 'r1')
      AS t(id, rv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_i AS
      SELECT * FROM VALUES
        (1, 'i1'),
        (2, 'i2')
      AS t(id, iv)
      """

    When query
      """
      SELECT
        l.id,
        l.lv,
        r.rv,
        i.iv
      FROM jric_l l
      LEFT JOIN jric_r r ON l.id = r.id
      JOIN jric_i i ON l.id = i.id
      ORDER BY l.id
      """
    Then query result ordered
      | id | lv | rv   | iv |
      | 1  | l1 | r1   | i1 |
      | 2  | l2 | NULL | i2 |

  Scenario: Inner-join chain with cross-relation non-equi pending filter
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_pf_a AS
      SELECT * FROM VALUES
        (1, 10),
        (2, 20),
        (3, 30)
      AS t(id, ax)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_pf_b AS
      SELECT * FROM VALUES
        (1, 5),
        (2, 25),
        (3, 1)
      AS t(id, by)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_pf_c AS
      SELECT * FROM VALUES
        (1, 40),
        (2, 40),
        (3, 40)
      AS t(id, cz)
      """

    # The predicate (a.ax + b.by) > c.cz references three relations and cannot be
    # attached at any binary join below the top of the reorderable region. It must
    # be deferred and then applied exactly once when the third relation joins in.
    When query
      """
      SELECT
        a.id,
        a.ax,
        b.by,
        c.cz
      FROM jric_pf_a a
      JOIN jric_pf_b b ON a.id = b.id
      JOIN jric_pf_c c ON a.id = c.id
      WHERE (a.ax + b.by) > c.cz
      ORDER BY a.id
      """
    Then query result ordered
      | id | ax | by | cz |
      | 2  | 20 | 25 | 40 |

  Scenario: Sort and Limit operators inside an inner-join chain are boundary leaves
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_sl_a AS
      SELECT * FROM VALUES
        (1, 'a1'),
        (2, 'a2'),
        (3, 'a3'),
        (4, 'a4')
      AS t(id, av)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_sl_b AS
      SELECT * FROM VALUES
        (1, 'b1'),
        (2, 'b2'),
        (3, 'b3'),
        (4, 'b4')
      AS t(id, bv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_sl_c AS
      SELECT * FROM VALUES
        (1, 'c1'),
        (2, 'c2'),
        (3, 'c3'),
        (4, 'c4')
      AS t(id, cv)
      """

    # The inner subquery introduces Sort+Limit which the join-reorder graph builder must
    # treat as an opaque boundary leaf. Reorder may still happen around it, but the
    # Sort+Limit subtree must not be descended into.
    When query
      """
      SELECT a.id, a.av, sub.bv, c.cv
      FROM jric_sl_a a
      JOIN (SELECT id, bv FROM jric_sl_b ORDER BY id LIMIT 2) sub ON a.id = sub.id
      JOIN jric_sl_c c ON a.id = c.id
      ORDER BY a.id
      """
    Then query result ordered
      | id | av | bv | cv |
      | 1  | a1 | b1 | c1 |
      | 2  | a2 | b2 | c2 |

  Scenario: Union and Window operators inside inner-join chain stay opaque
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_uw_a AS
      SELECT * FROM VALUES
        (1, 100),
        (2, 200),
        (3, 300)
      AS t(id, ax)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_uw_b1 AS
      SELECT * FROM VALUES
        (1, 'b1a'),
        (2, 'b1b')
      AS t(id, bv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_uw_b2 AS
      SELECT * FROM VALUES
        (3, 'b2c')
      AS t(id, bv)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jric_uw_c AS
      SELECT * FROM VALUES
        (1, 'c1'),
        (2, 'c2'),
        (3, 'c3')
      AS t(id, cv)
      """

    # UNION ALL produces a UnionExec that must be a boundary leaf for the join graph
    # builder. The window function inside `wc` produces a WindowAggExec / projection
    # over a non-trivial expression, which must also stay opaque.
    When query
      """
      WITH wc AS (
        SELECT id, cv,
               ROW_NUMBER() OVER (PARTITION BY cv ORDER BY id) AS rn
        FROM jric_uw_c
      )
      SELECT a.id, a.ax, ub.bv, wc.cv, wc.rn
      FROM jric_uw_a a
      JOIN (SELECT id, bv FROM jric_uw_b1 UNION ALL SELECT id, bv FROM jric_uw_b2) ub
        ON a.id = ub.id
      JOIN wc ON a.id = wc.id
      ORDER BY a.id
      """
    Then query result ordered
      | id | ax  | bv  | cv | rn |
      | 1  | 100 | b1a | c1 | 1  |
      | 2  | 200 | b1b | c2 | 1  |
      | 3  | 300 | b2c | c3 | 1  |
