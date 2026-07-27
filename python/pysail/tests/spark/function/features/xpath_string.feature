Feature: xpath_string with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: xpath_string — the argument must be foldable

    @column_args
    Scenario: xpath_string with the argument as a literal
      When query
        """
        SELECT xpath_string('<a><b>b</b><c>cc</c></a>','a/c') AS result
        """
      Then query result ordered
        | result |
        | cc     |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['cc', 'NULL'].
    @column_args @sail-bug
    Scenario: xpath_string takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath_string('<a><b>b</b><c>cc</c></a>', c) AS result FROM VALUES (1, 'a/c'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['cc', 'cc'].
    @column_args @sail-bug
    Scenario: xpath_string takes argument 2 from a column
      When query
        """
        SELECT xpath_string('<a><b>b</b><c>cc</c></a>', c) AS result FROM VALUES (1, 'a/c'), (2, 'a/c') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT
