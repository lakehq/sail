Feature: Datetime functions

  Rule: weekday

    Scenario: weekday sunday is 6
      When query
        """
        SELECT weekday(DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: null handling

    Scenario: year of null
      When query
        """
        SELECT year(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: month of null
      When query
        """
        SELECT month(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: datediff with null
      When query
        """
        SELECT datediff(NULL, DATE '2024-03-15') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: last_day of null
      When query
        """
        SELECT last_day(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: next_day null handling

    Scenario: next_day with null day of week
      When query
        """
        SELECT next_day(DATE '2024-03-15', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |
