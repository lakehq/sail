Feature: string padding functions

  Rule: Binary and string argument dispatch

    Scenario: binary input with string pad uses string padding
      When query
      """
      SELECT
        lpad(CAST('hi' AS BINARY), 5, '?') AS left_value,
        rpad(CAST('hi' AS BINARY), 5, '?') AS right_value
      """
      Then query result
      | left_value | right_value |
      | ???hi      | hi???       |
