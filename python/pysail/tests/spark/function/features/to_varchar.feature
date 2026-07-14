Feature: to_varchar with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: to_varchar — the argument must be foldable

    @column_args
    Scenario: to_varchar with the argument as a literal
      When query
        """
        SELECT to_varchar(454, '999') AS result
        """
      Then query result ordered
        | result |
        | 454    |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_varchar takes argument 2 from a column holding two different values
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, '000D00') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_varchar takes argument 2 from a column containing NULL
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_varchar takes argument 2 from a column
      When query
        """
        SELECT to_varchar(454, c) AS result FROM VALUES (1, '999'), (2, '999') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT
