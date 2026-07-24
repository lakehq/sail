Feature: to_char with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: to_char — a numeric format must be foldable

    @column_args
    Scenario: to_char with the argument as a literal
      When query
        """
        SELECT to_char(454, '999') AS result
        """
      Then query result ordered
        | result |
        | 454    |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column holding two different values
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, '000D00') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column containing NULL
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['454', '454'].
    @column_args @sail-bug
    Scenario: to_char takes argument 2 from a column
      When query
        """
        SELECT to_char(454, c) AS result FROM VALUES (1, '999'), (2, '999') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  # With a DATE or TIMESTAMP input, Spark resolves `to_char` to `DateFormatClass`, which accepts a
  # non-foldable format and applies it row by row. So the foldable rule above is specific to the
  # numeric format; here the column is legal and each row must use its own format.
  Rule: to_char — a date format is resolved per row

    # Sail applies the first row's value to every row: Sail returns ['2026', '2026'].
    @column_args @sail-bug
    Scenario: to_char takes the date format from a column holding two different values
      When query
        """
        SELECT to_char(DATE '2026-02-02', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |
