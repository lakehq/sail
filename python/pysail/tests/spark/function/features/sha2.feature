Feature: sha2 with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: sha2 — the argument may come from a column

    @column_args
    Scenario: sha2 with the argument as a literal
      When query
        """
        SELECT sha2('Spark', 256) AS result
        """
      Then query result ordered
        | result                                                           |
        | 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b |

    # Sail rejects the column: Sail errors: invalid argument: The second argument of sha2 must be a literal integer.
    @column_args @sail-bug
    Scenario: sha2 takes argument 2 from a column
      When query
        """
        SELECT sha2('Spark', c) AS result FROM VALUES (1, 256), (2, 256) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                                           |
        | 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b |
        | 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b |

    @column_args @sail-bug
    Scenario: sha2 takes argument 2 from a column holding two different values
      When query
        """
        SELECT sha2('Spark', c) AS result FROM VALUES (1, 256), (2, 512) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                                                                                                           |
        | 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b                                                                 |
        | 44844a586c54c9a212da1dbfe05c5f1705de1af5fda1f0d36297623249b279fd8f0ccec03f888f4fb13bf7cd83fdad58591c797f81121a23cfdd5e0897795238 |
