Feature: Broadcast hint in explain plans

  Scenario: BROADCAST hint selects collect-left hash join mode
    When query
      """
      EXPLAIN
      SELECT /*+ BROADCAST(right_tbl) */
        left_tbl.id
      FROM
        (SELECT * FROM VALUES (1), (2), (3) AS t(id)) AS left_tbl
      JOIN
        (SELECT * FROM VALUES (1), (2), (3) AS t(id)) AS right_tbl
      ON left_tbl.id = right_tbl.id
      """
    Then query plan contains "HashJoinExec: mode=CollectLeft"
