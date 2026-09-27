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
