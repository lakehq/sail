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

    Scenario: bin truncates decimal strings before converting to binary
      When query
      """
      SELECT bin(value) AS result
      FROM VALUES (0, '13.9'), (1, '-13.9'), (2, '.3') AS data(id, value)
      ORDER BY id
      """
      Then query result
      | result                                                           |
      | 1101                                                             |
      | 1111111111111111111111111111111111111111111111111111111111110011 |
      | 0                                                                |
