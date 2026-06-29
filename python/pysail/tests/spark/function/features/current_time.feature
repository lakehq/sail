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
