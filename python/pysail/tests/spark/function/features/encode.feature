Feature: encode function

  Rule: Named arguments

    Scenario: encode accepts Spark named arguments in any order
      When query
      """
      SELECT base64(encode(charset => 'utf-8', value => 'ab')) AS result
      """
      Then query result
      | result |
      | YWI=   |
