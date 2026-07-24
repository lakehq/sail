Feature: unix_timestamp with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: unix_timestamp — the argument may come from a column

    @column_args
    Scenario: unix_timestamp with the argument as a literal
      When query
        """
        SELECT unix_timestamp('2016-04-08', 'yyyy-MM-dd') AS result
        """
      Then query result ordered
        | result     |
        | 1460073600 |

    # Sail rejects the column: Sail errors: Unsupported data type Array(StringArray [ "%Y-%m-%d", "%Y-%m-%d", ]) for function to_times...
    @column_args @sail-bug
    Scenario: unix_timestamp takes argument 2 from a column
      When query
        """
        SELECT unix_timestamp('2016-04-08', c) AS result FROM VALUES (1, 'yyyy-MM-dd'), (2, 'yyyy-MM-dd') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1460073600 |
        | 1460073600 |
