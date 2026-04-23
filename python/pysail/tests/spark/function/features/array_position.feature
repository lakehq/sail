@array_position
Feature: array_position returns Long (Int64) matching Spark's LongType

  Rule: Basic usage

    Scenario: array_position returns 0 for element not found
      When query
        """
        SELECT array_position(array(312, 773, 708, 708), 414) AS v
        """
      Then query result
        | v |
        | 0 |

    Scenario: array_position returns 1-based index of first match
      When query
        """
        SELECT array_position(array(312, 773, 708, 708), 708) AS v
        """
      Then query result
        | v |
        | 3 |

  Rule: Return type is Long (Int64)

    Scenario: array_position result can be cast to Long without error
      When query
        """
        SELECT CAST(array_position(array(1, 2, 3), 2) AS BIGINT) AS v
        """
      Then query result
        | v |
        | 2 |

    Scenario: array_position on empty array returns 0
      When query
        """
        SELECT array_position(array(), 1) AS v
        """
      Then query result
        | v |
        | 0 |

    Scenario: array_position on NULL array returns NULL
      When query
        """
        SELECT array_position(CAST(NULL AS ARRAY<INT>), 1) AS v
        """
      Then query result
        | v    |
        | NULL |
