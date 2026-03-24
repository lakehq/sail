Feature: Join reorder preserves join filters referencing derived columns

  Scenario: Join filter depends on derived projection column and must not be pushed too early
    # ec_t1 is large so the optimizer is incentivized to reorder and join ec_t2/ec_t3 first.
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW ec_t1 AS
      SELECT
        CAST(id AS INT) AS id,
        CAST(id AS INT) AS a,
        CAST(id * 10 AS INT) AS b
      FROM range(0, 1000)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW ec_t2 AS
      SELECT * FROM VALUES
        (1),
        (2)
      AS t(id)
      """
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW ec_t3 AS
      SELECT * FROM VALUES
        (1, 5),
        (2, 25)
      AS t(id, c)
      """

    # The final join contains a non-equi filter referencing `sub.derived_col`, which is a derived
    # expression column. JoinReorder must track that dependency so it doesn't apply the filter
    # before `sub` is part of the current join.
    When query
      """
      WITH sub AS (
        SELECT
          id,
          (a + b) AS derived_col
        FROM ec_t1
      )
      SELECT
        sub.id,
        sub.derived_col,
        t3.c
      FROM sub
      JOIN ec_t2 t2 ON sub.id = t2.id
      JOIN ec_t3 t3 ON t2.id = t3.id AND sub.derived_col > t3.c
      ORDER BY sub.id
      """
    Then query result ordered
      | id | derived_col | c |
      | 1  | 11          | 5 |

