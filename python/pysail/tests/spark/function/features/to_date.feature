@to_date
Feature: to_date (strict variant)
  Strict to_date that throws an error on invalid input,
  contrasting with try_to_date which returns NULL.

  Rule: Valid input parses

    Scenario: ISO date
      When query
      """
      SELECT to_date('2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: With format
      When query
      """
      SELECT to_date('15/01/2024', 'dd/MM/yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Cast from timestamp
      When query
      """
      SELECT to_date(TIMESTAMP '2024-01-15 10:30:00') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

  Rule: Invalid input throws

    Scenario: Garbage string raises error
      When query
      """
      SELECT to_date('not-a-date')
      """
      Then query error CAST_INVALID_INPUT|error in SQL parser|cannot be cast

    Scenario: Format mismatch raises error
      When query
      """
      SELECT to_date('2024-01-15', 'dd/MM/yyyy')
      """
      Then query error invalid characters|CONVERSION_INVALID_INPUT|cannot be parsed

  Rule: NULL input propagates

    Scenario: NULL input returns NULL
      When query
      """
      SELECT to_date(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |
