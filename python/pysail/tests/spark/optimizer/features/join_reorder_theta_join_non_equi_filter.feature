Feature: Join reorder supports theta joins (non-equi filter without equi keys)

  Scenario: Complex join key prevents equi-pair reconstruction but query should still run
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW tj_t1 AS
      SELECT * FROM VALUES
        (1, 10),
        (2, 20)
      AS t(id, v1)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW tj_t2 AS
      SELECT
        CAST(id AS BIGINT) AS id,
        v2
      FROM VALUES
        (1, 1),
        (2, 2)
      AS t(id, v2)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW tj_t3 AS
      SELECT * FROM VALUES
        (1, 100),
        (2, 200)
      AS t(id, v3)
      """

    # The first join uses a complex join key (CAST), which prevents JoinReorder's graph builder
    # from recording equi_pairs, but the join predicate still exists and must be preserved.
    When query
      """
      SELECT
        t1.id,
        t2.v2,
        t3.v3
      FROM tj_t1 t1
      JOIN tj_t2 t2 ON CAST(t1.id AS BIGINT) = t2.id
      JOIN tj_t3 t3 ON t1.id = t3.id
      ORDER BY t1.id
      """
    Then query result ordered
      | id | v2 | v3  |
      | 1  | 1  | 100 |
      | 2  | 2  | 200 |

