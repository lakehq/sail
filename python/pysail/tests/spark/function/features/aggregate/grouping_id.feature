Feature: grouping_id returns Spark-compatible grouping-set bit masks

  Rule: grouping_id over grouping analytics

    Scenario: grouping_id and grouping over cube
      When query
      """
      SELECT
        name,
        height,
        grouping(name) AS g_name,
        grouping(height) AS g_height,
        grouping_id() AS gid,
        sum(age) AS total
      FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height)
      GROUP BY cube(name, height)
      ORDER BY gid, name, height
      """
      Then query result ordered
      | name  | height | g_name | g_height | gid | total |
      | Alice | 165    | 0      | 0        | 0   | 2     |
      | Bob   | 180    | 0      | 0        | 0   | 5     |
      | Alice | NULL   | 0      | 1        | 1   | 2     |
      | Bob   | NULL   | 0      | 1        | 1   | 5     |
      | NULL  | 165    | 1      | 0        | 2   | 2     |
      | NULL  | 180    | 1      | 0        | 2   | 5     |
      | NULL  | NULL   | 1      | 1        | 3   | 7     |

    Scenario: grouping_id accepts the complete grouping column list
      When query
      """
      SELECT name, height, grouping_id(name, height) AS gid, sum(age) AS total
      FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height)
      GROUP BY cube(name, height)
      ORDER BY gid, name, height
      """
      Then query result ordered
      | name  | height | gid | total |
      | Alice | 165    | 0   | 2     |
      | Bob   | 180    | 0   | 5     |
      | Alice | NULL   | 1   | 2     |
      | Bob   | NULL   | 1   | 5     |
      | NULL  | 165    | 2   | 2     |
      | NULL  | 180    | 2   | 5     |
      | NULL  | NULL   | 3   | 7     |

    Scenario: grouping_id returns long
      When query
      """
      SELECT grouping_id() AS gid
      FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name)
      GROUP BY cube(name)
      """
      Then query schema
      """
      root
       |-- gid: long (nullable = false)
      """

    Scenario: grouping_id works inside a window partition
      When query
      """
      SELECT
        name,
        height,
        grouping_id() AS gid,
        sum(age) AS total,
        rank() OVER (PARTITION BY grouping_id() ORDER BY sum(age)) AS r
      FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height)
      GROUP BY cube(name, height)
      ORDER BY gid, r, name, height
      """
      Then query result ordered
      | name  | height | gid | total | r |
      | Alice | 165    | 0   | 2     | 1 |
      | Bob   | 180    | 0   | 5     | 2 |
      | Alice | NULL   | 1   | 2     | 1 |
      | Bob   | NULL   | 1   | 5     | 2 |
      | NULL  | 165    | 2   | 2     | 1 |
      | NULL  | 180    | 2   | 5     | 2 |
      | NULL  | NULL   | 3   | 7     | 1 |

    Scenario: grouping_id rejects mismatched columns
      When query
      """
      SELECT grouping_id(age) AS gid
      FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height)
      GROUP BY cube(name, height)
      """
      Then query error GROUPING_ID_COLUMN_MISMATCH
