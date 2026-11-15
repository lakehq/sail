Feature: Join reorder projection rewrite supports nested derived expressions

  Scenario: Derived alias from subquery participates in outer expression
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jr_t1 AS
      SELECT * FROM VALUES
        (1, 10),
        (2, 20)
      AS t(id, uid)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jr_t2 AS
      SELECT * FROM VALUES
        (1, 5),
        (2, 6)
      AS t(id, inc)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW jr_t3 AS
      SELECT * FROM VALUES
        (1, 7),
        (2, 8)
      AS t(id, extra)
      """

    When query
      """
      SELECT
        id,
        (new_uid_val + extra) AS result
      FROM (
        SELECT
          t1.id AS id,
          (t1.uid + t2.inc) AS new_uid_val,
          t3.extra AS extra
        FROM jr_t1 t1
        JOIN jr_t2 t2 ON t1.id = t2.id
        JOIN jr_t3 t3 ON t1.id = t3.id
      ) q
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 1  | 22     |
      | 2  | 34     |

