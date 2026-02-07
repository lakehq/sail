Feature: min_by function

  Rule: min_by with all NULLs in ordering column

    Scenario: min_by with all NULLs in ordering column
      When query
      """
      SELECT min_by(name, age) AS result
      FROM VALUES ('Alice', CAST(NULL AS INT)), ('Bob', CAST(NULL AS INT)) AS t(name, age)
      """
      Then query result
      | result |
      | NULL   |

  Rule: min_by as window function

    Scenario: min_by over window
      When query
      """
      SELECT name, age,
             min_by(name, age) OVER (ORDER BY age ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
      FROM VALUES ('Alice', 30), ('Bob', 50), ('Carol', 40) AS t(name, age)
      ORDER BY age
      """
      Then query result ordered
      | name  | age | result |
      | Alice | 30  | Alice  |
      | Carol | 40  | Alice  |
      | Bob   | 50  | Alice  |
