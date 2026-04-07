Feature: regr_avgy returns the average of the dependent variable for non-null pairs

  Rule: regr_avgy computes the average of y for non-null (y, x) pairs

    Scenario: all pairs are non-null
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x)
        """
      Then query result
        | result |
        | 1.75   |

    Scenario: some x values are null
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x)
        """
      Then query result
        | result             |
        | 1.6666666666666667 |

    Scenario: some x or y values are null
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x)
        """
      Then query result
        | result |
        | 1.5    |

  Rule: regr_avgy returns NULL when all pairs are excluded

    Scenario: all x values are null
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1, null) AS tab(y, x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: all y values are null
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (null, 1) AS tab(y, x)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: empty table
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1, 2) AS tab(y, x)
        WHERE false
        """
      Then query result
        | result |
        | NULL   |

  Rule: regr_avgy works with different numeric types

    Scenario: integer inputs
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (CAST(10 AS INT), CAST(1 AS INT)), (CAST(20 AS INT), CAST(2 AS INT)) AS tab(y, x)
        """
      Then query result
        | result |
        | 15.0   |

    Scenario: double inputs
      When query
        """
        SELECT regr_avgy(y, x) AS result
        FROM VALUES (1.5, 2.0), (2.5, 3.0) AS tab(y, x)
        """
      Then query result
        | result |
        | 2.0    |

  Rule: regr_avgy works as a window function

    Scenario: regr_avgy over window
      When query
        """
        SELECT y, x,
               regr_avgy(y, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
        FROM VALUES (1, 2), (2, 3), (3, 4) AS tab(y, x)
        ORDER BY x
        """
      Then query result ordered
        | y | x | result |
        | 1 | 2 | 1.0    |
        | 2 | 3 | 1.5    |
        | 3 | 4 | 2.0    |
