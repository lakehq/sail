Feature: to_date with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_date — the argument may come from a column

    @column_args
    Scenario: to_date with the argument as a literal
      When query
        """
        SELECT to_date('2016-12-31', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |

    # Sail rejects the column: Sail errors: Unsupported data type Array(StringArray [ "%Y-%m-%d", "%Y-%m-%d", ]) for function to_date,...
    @column_args @sail-bug
    Scenario: to_date takes argument 2 from a column
      When query
        """
        SELECT to_date('2016-12-31', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2016-12-31 |
        | 2016-12-31 |
