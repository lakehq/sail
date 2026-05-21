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
