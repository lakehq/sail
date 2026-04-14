@try_to_date
Feature: try_to_date
  Safe variant of to_date that returns NULL on parse failure
  instead of throwing an exception.

  Rule: Single-argument form uses default parsers

    Scenario: ISO date string parses
      When query
      """
      SELECT try_to_date('2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Beginning of epoch parses
      When query
      """
      SELECT try_to_date('1970-01-01') AS result
      """
      Then query result
      | result     |
      | 1970-01-01 |

    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_date('not-a-date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_date('') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL input returns NULL
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Cast from timestamp preserves date
      When query
      """
      SELECT try_to_date(TIMESTAMP '2024-01-15 10:30:00') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

  Rule: Two-argument form parses with chrono format

    Scenario: Custom format parses
      When query
      """
      SELECT try_to_date('2024/01/15', '%Y/%m/%d') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Invalid input with format returns NULL
      When query
      """
      SELECT try_to_date('garbage', '%Y/%m/%d') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_date('2024-01-15', '%Y/%m/%d') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL input with format returns NULL
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING), '%Y/%m/%d') AS result
      """
      Then query result
      | result |
      | NULL   |
