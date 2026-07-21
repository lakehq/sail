@xpath
Feature: xpath_short with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: xpath_short — the argument must be foldable

    @column_args
    Scenario: xpath_short with the argument as a literal
      When query
        """
        SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result ordered
        | result |
        | 3      |

    @column_args
    Scenario: xpath_short takes argument 2 from a column containing NULL
      When query
        """
        SELECT xpath_short('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    @column_args
    Scenario: xpath_short takes argument 2 from a column
      When query
        """
        SELECT xpath_short('<a><b>1</b><b>2</b></a>', c) AS result FROM VALUES (1, 'sum(a/b)'), (2, 'sum(a/b)') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT
