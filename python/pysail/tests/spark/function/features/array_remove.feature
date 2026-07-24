Feature: array_remove with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: array_remove — the argument may come from a column

    @column_args
    Scenario: array_remove with the argument as a literal
      When query
        """
        SELECT array_remove(array(1, 2, 3, null, 3), 3) AS result
        """
      Then query result ordered
        | result       |
        | [1, 2, NULL] |

    # Sail rejects the column: Sail errors: Invalid argument error: Column '#4' is declared as non-nullable but contains null values
    @column_args @sail-bug
    Scenario: array_remove takes argument 2 from a column containing NULL
      When query
        """
        SELECT array_remove(array(1, 2, 3, null, 3), c) AS result FROM VALUES (1, 3), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result       |
        | [1, 2, NULL] |
        | NULL         |

    @column_args
    Scenario: array_remove takes argument 2 from a column holding two different values
      When query
        """
        SELECT array_remove(array(1, 2, 3), c) AS result FROM VALUES (1, 1), (2, 3) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [2, 3] |
        | [1, 2] |
