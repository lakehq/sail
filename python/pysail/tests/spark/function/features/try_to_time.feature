@try_to_time
Feature: try_to_time
  Safe variant of to_time that returns NULL on parse failure
  instead of throwing an exception.

  Rule: Single-argument form uses default parsers

    Scenario: HH:MM:SS format parses
      When query
      """
      SELECT try_to_time('10:30:45') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    Scenario: Midnight parses
      When query
      """
      SELECT try_to_time('00:00:00') AS result
      """
      Then query result
      | result   |
      | 00:00:00 |

    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_time('not-a-time') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_time('') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Out-of-range hour returns NULL
      When query
      """
      SELECT try_to_time('25:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL input returns NULL
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Two-argument form parses with chrono format

    Scenario: Custom format parses
      When query
      """
      SELECT try_to_time('10-30-45', '%H-%M-%S') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_time('10:30:45', '%H-%M-%S') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL with format returns NULL
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING), '%H:%M:%S') AS result
      """
      Then query result
      | result |
      | NULL   |
