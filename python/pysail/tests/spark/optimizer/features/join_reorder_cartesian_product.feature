Feature: Join reorder greedy solver handles cartesian products

  Scenario: Disconnected join graph requires cartesian join but should not crash
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW cj_t1 AS
      SELECT * FROM VALUES
        (1, 10),
        (2, 20)
      AS t(id, v1)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW cj_t2 AS
      SELECT * FROM VALUES
        (1, 1),
        (2, 2)
      AS t(id, v2)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW cj_t3 AS
      SELECT * FROM VALUES
        (100),
        (200)
      AS t(v3)
      """

    # Force join reorder to consider 3 relations, but only one join edge exists (t1.id = t2.id).
    # The remaining relation must be combined via cartesian product without planner crash.
    When query
      """
      SELECT
        t1.id,
        t1.v1,
        t2.v2,
        t3.v3
      FROM cj_t1 t1
      JOIN cj_t2 t2 ON t1.id = t2.id
      JOIN cj_t3 t3
      ORDER BY t1.id, t3.v3
      """
    Then query result ordered
      | id | v1 | v2 | v3  |
      | 1  | 10 | 1  | 100 |
      | 1  | 10 | 1  | 200 |
      | 2  | 20 | 2  | 100 |
      | 2  | 20 | 2  | 200 |

