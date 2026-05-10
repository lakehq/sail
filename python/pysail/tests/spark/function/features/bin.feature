Feature: bin converts integral values to binary strings

  Rule: String arguments follow Spark cast semantics

    Scenario: bin returns null for strings that cannot be cast to integers
      When query
      """
      SELECT bin(value) AS result
      FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS data(value)
      ORDER BY value IS NULL, value
      """
      Then query result
      | result |
      | NULL   |
      | NULL   |

    Scenario: bin trims strings before casting to integers
      When query
      """
      SELECT bin(value) AS result
      FROM VALUES (' 13 '), (' -13 ') AS data(value)
      ORDER BY value
      """
      Then query result
      | result                                                           |
      | 1111111111111111111111111111111111111111111111111111111111110011 |
      | 1101                                                             |
