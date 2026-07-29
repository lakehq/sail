@mode
Feature: mode

  Rule: Result values (migrated from test_mode.txt doctests)

    Scenario: mode doctest #1 (result)
      When query
        """
        SELECT mode(col) FROM VALUES (0), (10), (10) AS tab(col)
        """
      Then query result
        | mode(col) |
        | 10        |

    Scenario Outline: mode doctest <case> (result)
      When query
        """
        SELECT mode() WITHIN GROUP (ORDER BY col) FROM VALUES <values> AS tab(col)
        """
      Then query result
        | mode() WITHIN GROUP (ORDER BY col DESC) |
        | 10                                      |

      Examples:
        | case | values                      |
        | #2   | (0), (10), (10), (20), (20) |
        | #3   | (0), (10), (10)             |

    Scenario: mode doctest #4 (result)
      When query
        """
        SELECT mode() WITHIN GROUP (ORDER BY col DESC) FROM VALUES (0), (10), (10), (20), (20) AS tab(col)
        """
      Then query result
        | mode() WITHIN GROUP (ORDER BY col) |
        | 20                                 |
