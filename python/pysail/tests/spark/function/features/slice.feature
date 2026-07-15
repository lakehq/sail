Feature: slice with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: slice — the argument may come from a column

    @column_args
    Scenario: slice with the argument as a literal
      When query
        """
        SELECT slice(array(1, 2, 3, 4), 2, 2) AS result
        """
      Then query result ordered
        | result |
        | [2, 3] |

    # Sail rejects the column: Sail errors: Invalid argument error: Non-nullable field of ListArray "item" cannot contain nulls
    @column_args @sail-bug
    Scenario: slice takes argument 2 from a column containing NULL
      When query
        """
        SELECT slice(array(1, 2, 3, 4), c, 2) AS result FROM VALUES (1, 2), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [2, 3] |
        | NULL   |

    # Sail rejects the column: Sail errors: Invalid argument error: Non-nullable field of ListArray "item" cannot contain nulls
    @column_args @sail-bug
    Scenario: slice takes argument 3 from a column containing NULL
      When query
        """
        SELECT slice(array(1, 2, 3, 4), 2, c) AS result FROM VALUES (1, 2), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [2, 3] |
        | NULL   |

    @column_args
    Scenario: slice takes argument 2 from a column holding two different values
      When query
        """
        SELECT slice(array(1, 2, 3, 4), c, 2) AS result FROM VALUES (1, 1), (2, 3) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [1, 2] |
        | [3, 4] |
