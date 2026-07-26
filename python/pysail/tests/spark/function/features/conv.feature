Feature: conv with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: conv — the argument may come from a column

    @column_args
    Scenario: conv with the argument as a literal
      When query
        """
        SELECT conv('100', 2, 10) AS result
        """
      Then query result ordered
        | result |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @column_args @sail-bug
    Scenario: conv takes argument 2 from a column holding two different values
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 16) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 256    |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @column_args @sail-bug
    Scenario: conv takes argument 2 from a column
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 2) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @column_args @sail-bug
    Scenario: conv takes argument 3 from a column
      When query
        """
        SELECT conv('100', 2, c) AS result FROM VALUES (1, 10), (2, 10) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |
