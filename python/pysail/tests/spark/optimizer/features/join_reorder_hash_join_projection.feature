Feature: Join reorder respects HashJoinExec projection

  Scenario: Join reorder preserves correct column mapping when a join has projection
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW hjp_t1 AS
      SELECT * FROM VALUES
        (1, 10, 100),
        (2, 20, 200)
      AS t(id, a, a2)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW hjp_t2 AS
      SELECT * FROM VALUES
        (1, 1, 11),
        (2, 2, 22)
      AS t(id, b, d)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW hjp_t3 AS
      SELECT * FROM VALUES
        (1, 7),
        (2, 8)
      AS t(id, x)
      """

    # Subquery introduces a projection over the first join (reorder + drop columns).
    # DataFusion may embed this projection into HashJoinExec.projection.
    When query
      """
      WITH j12 AS (
        SELECT
          t2.d AS d,
          t1.a AS a,
          t2.id AS id
        FROM hjp_t1 t1
        JOIN hjp_t2 t2 ON t1.id = t2.id
      )
      SELECT
        j12.id,
        j12.a,
        j12.d,
        t3.x
      FROM j12
      JOIN hjp_t3 t3 ON j12.id = t3.id
      ORDER BY j12.id
      """
    Then query result ordered
      | id | a  | d  | x |
      | 1  | 10 | 11 | 7 |
      | 2  | 20 | 22 | 8 |

