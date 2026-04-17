Feature: position() finds the position of a substring in a string

  Rule: Basic usage

    Scenario: position with substring found
      When query
      """
      SELECT position('a', 'bbba') AS v
      """
      Then query result
      | v |
      | 4 |

    Scenario: position with substring not found
      When query
      """
      SELECT position('x', 'bbba') AS v
      """
      Then query result
      | v |
      | 0 |

  Rule: Zero or negative start position returns 0

    Scenario: position with start position 0 returns 0
      When query
      """
      SELECT position('a', 'bbba', 0) AS v
      """
      Then query result
      | v |
      | 0 |

    Scenario: position with start position -1 returns 0
      When query
      """
      SELECT position('a', 'bbba', -1) AS v
      """
      Then query result
      | v |
      | 0 |

    Scenario: position with start position -2 returns 0
      When query
      """
      SELECT position('a', 'bbba', -2) AS v
      """
      Then query result
      | v |
      | 0 |

  Rule: Positive start position

    Scenario: position with start position 1 finds substring
      When query
      """
      SELECT position('a', 'bbba', 1) AS v
      """
      Then query result
      | v |
      | 4 |

    Scenario: position with start position past the match returns 0
      When query
      """
      SELECT position('a', 'bbba', 5) AS v
      """
      Then query result
      | v |
      | 0 |
