Feature: get with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: get — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: get with the argument as a literal
      When query
        """
        SELECT get(array(1, 2, 3), 0) AS result
        """
      Then query result ordered
        | result |
        | 1      |

    # Sail applies the first row's value to every row: Sail returns ['1', '1'].
    @column_args @sail-bug
    Scenario: get takes argument 2 from a column containing NULL
      When query
        """
        SELECT get(array(1, 2, 3), c) AS result FROM VALUES (1, 0), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 1      |
        | NULL   |

    @column_args
    Scenario: get takes argument 2 from a column holding two different values
      When query
        """
        SELECT get(array(10, 20, 30), c) AS result FROM VALUES (1, 0), (2, 2) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 10     |
        | 30     |
