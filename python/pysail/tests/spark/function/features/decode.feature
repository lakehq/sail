Feature: decode function

  Rule: Conditional decode

    Scenario: decode without default returns a string null
      When query
      """
      SELECT decode(6, 1, 'Southlake', 2, 'San Francisco') AS result
      """
      Then query schema
      """
      root
       |-- result: string (nullable = true)
      """
      Then query result
      | result |
      | NULL   |
