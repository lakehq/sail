Feature: Semi and anti join with qualified attribute names

  Rule: Left semi join

    Scenario: left semi join filters matching rows
      When query
      """
      SELECT * FROM (
        SELECT t1.id, t1.name FROM
          (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob' UNION ALL SELECT 3, 'Carol') t1
        LEFT SEMI JOIN
          (SELECT 1 AS id UNION ALL SELECT 3 AS id) t2
        ON t1.id = t2.id
      )
      ORDER BY id
      """
      Then query result
      | id | name  |
      | 1  | Alice |
      | 3  | Carol |

  Rule: Left anti join

    Scenario: left anti join filters non-matching rows
      When query
      """
      SELECT * FROM (
        SELECT t1.id, t1.name FROM
          (SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob' UNION ALL SELECT 3, 'Carol') t1
        LEFT ANTI JOIN
          (SELECT 1 AS id UNION ALL SELECT 3 AS id) t2
        ON t1.id = t2.id
      )
      ORDER BY id
      """
      Then query result
      | id | name |
      | 2  | Bob  |

    Scenario: left anti join with no matches returns all rows
      When query
      """
      SELECT * FROM (
        SELECT t1.id FROM
          (SELECT 1 AS id UNION ALL SELECT 2 AS id) t1
        LEFT ANTI JOIN
          (SELECT 99 AS id) t2
        ON t1.id = t2.id
      )
      ORDER BY id
      """
      Then query result
      | id |
      | 1  |
      | 2  |
