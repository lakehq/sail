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
        | 10 |

    Scenario: mode doctest #2 (result)
      When query
        """
        SELECT mode() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10), (20), (20) AS tab(col)
        """
      Then query result
        | mode() WITHIN GROUP (ORDER BY col DESC) |
        | 10 |

    Scenario: mode doctest #3 (result)
      When query
        """
        SELECT mode() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10) AS tab(col)
        """
      Then query result
        | mode() WITHIN GROUP (ORDER BY col DESC) |
        | 10 |

    Scenario: mode doctest #4 (result)
      When query
        """
        SELECT mode() WITHIN GROUP (ORDER BY col DESC) FROM VALUES (0), (10), (10), (20), (20) AS tab(col)
        """
      Then query result
        | mode() WITHIN GROUP (ORDER BY col) |
        | 20 |

