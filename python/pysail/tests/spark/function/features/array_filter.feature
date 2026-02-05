Feature: array filter with lambda

  Rule: Filter array elements using lambda predicates

    Scenario: Filter integers greater than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x < 3) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter integers greater than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x >= 3) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x <= 2) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter where all elements match
      When query
        """
        SELECT filter(array(10, 20, 30), x -> x > 5) AS result
        """
      Then query result
        | result       |
        | [10, 20, 30] |

    Scenario: Filter where no elements match
      When query
        """
        SELECT filter(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Filter with reversed comparison
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> 3 < x) AS result
        """
      Then query result
        | result |
        | [4, 5] |
