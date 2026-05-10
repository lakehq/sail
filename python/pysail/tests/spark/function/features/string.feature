Feature: String functions

  Rule: base64 null handling

    Scenario: base64 of null
      When query
        """
        SELECT base64(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: char and chr edge cases

    Scenario: char of negative
      When query
        """
        SELECT char(-1) AS result
        """
      Then query result
        | result |
        |        |

  Rule: left and right with negative length

    Scenario: left with negative
      When query
        """
        SELECT left('hello', -1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: right with negative
      When query
        """
        SELECT right('hello', -1) AS result
        """
      Then query result
        | result |
        |        |

  Rule: substring with negative start

    Scenario: substring negative start
      When query
        """
        SELECT substring('hello', -2) AS result
        """
      Then query result
        | result |
        | lo     |
