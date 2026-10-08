Feature: Anonymous Union alias resolution

  Scenario: Anonymous Union aliases preserve column values
    When query
      """
      SELECT sum(x) AS n
      FROM (SELECT id AS x FROM range(1000) UNION ALL SELECT id AS x FROM range(1000))
      """
    Then query result
      | n      |
      | 999000 |
    When query
      """
      EXPLAIN
      SELECT sum(x) AS n
      FROM (SELECT id AS x FROM range(1000) UNION ALL SELECT id AS x FROM range(1000))
      """
    Then query plan matches snapshot

  Scenario: Anonymous Union aliases preserve lambda inputs
    When query
      """
      SELECT sum(xs[0]) AS n
      FROM (
        SELECT transform(array(id, id + 1), x -> x + 10) AS xs FROM range(2)
        UNION ALL
        SELECT transform(array(id, id + 2), x -> x * 2) AS xs FROM range(2)
      )
      """
    Then query result
      | n  |
      | 23 |

  Scenario: Nested anonymous derived tables add no alias layers
    When query
      """
      SELECT __auto_generated_subquery_name.x
      FROM (SELECT * FROM (SELECT * FROM (SELECT id AS x FROM range(3))))
      """
    Then query result
      | x |
      | 0 |
      | 1 |
      | 2 |
    When query
      """
      EXPLAIN EXTENDED
      SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT id AS x FROM range(3))))
      """
    Then query plan matches snapshot
