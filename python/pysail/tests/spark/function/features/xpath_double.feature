Feature: xpath_double with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: xpath_double — the argument must be foldable

    @column_args
    Scenario: xpath_double with the argument as a literal
      When query
        """
        SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result ordered
        | result |
        | 3.0    |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', 'NULL'].
    @column_args @sail-bug
    Scenario: xpath_double takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath_double('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', '3.0'].
    @column_args @sail-bug
    Scenario: xpath_double takes argument 2 from a column
      When query
        """
        SELECT xpath_double('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, 'sum(a/b)') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT
