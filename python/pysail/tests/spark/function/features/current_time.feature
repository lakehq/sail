@spark-4.1
Feature: current_time function

  Rule: Precision argument validation

    Scenario: current_time accepts foldable precision expressions
      When query
      """
      SELECT current_time(1 + 1) IS NOT NULL AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: current_time preserves Spark precision in the schema
      When query
      """
      SELECT
        current_time(1) AS t1,
        current_time(2) AS t2,
        current_time(4) AS t4,
        current_time(5) AS t5
      """
      Then query schema
      """
      root
       |-- t1: time(1) (nullable = false)
       |-- t2: time(2) (nullable = false)
       |-- t4: time(4) (nullable = false)
       |-- t5: time(5) (nullable = false)
      """

    Scenario: current_time rejects out of range precision
      When query
      """
      SELECT current_time(7)
      """
      Then query error (?i)precision

    Scenario: current_time rejects null precision
      When query
      """
      SELECT current_time(NULL)
      """
      Then query error (?i)null|precision

    Scenario: current_time rejects non-foldable precision
      When query
      """
      SELECT current_time(id + 0) FROM VALUES (1) AS t(id)
      """
      Then query error (?i)foldable|precision
