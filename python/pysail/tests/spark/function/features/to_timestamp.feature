@to_timestamp
Feature: to_timestamp (strict variant)
  Strict to_timestamp that throws on invalid input,
  contrasting with try_to_timestamp which returns NULL.

  Rule: Valid input parses

    Scenario: ISO timestamp
      When query
      """
      SELECT to_timestamp('2024-01-15 10:30:45') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:30:45 |

    Scenario: Date-only parses with midnight
      When query
      """
      SELECT to_timestamp('2024-01-15') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: With format
      When query
      """
      SELECT to_timestamp('2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:30:45 |

    Scenario: Cast from date
      When query
      """
      SELECT to_timestamp(DATE '2024-01-15') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

  Rule: Invalid input throws

    Scenario: Garbage string raises error
      When query
      """
      SELECT to_timestamp('not-a-timestamp')
      """
      Then query error CAST_INVALID_INPUT|cannot be cast|error parsing|error in SQL parser

    Scenario: Format mismatch raises error
      When query
      """
      SELECT to_timestamp('2024-01-15', 'dd/MM/yyyy')
      """
      Then query error CANNOT_PARSE_TIMESTAMP|invalid characters|could not be parsed

  Rule: NULL input propagates

    Scenario: NULL input returns NULL
      When query
      """
      SELECT to_timestamp(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
      """
      SELECT to_timestamp(d, f) AS result FROM VALUES
        ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
        ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
      """
      Then query result
      | result              |
      | 2024-01-15 10:30:00 |
      | 2024-01-15 10:30:00 |
