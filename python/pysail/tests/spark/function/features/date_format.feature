Feature: date_format with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: date_format — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: date_format with the argument as a literal
      When query
        """
        SELECT date_format('2016-04-08', 'y') AS result
        """
      Then query result ordered
        | result |
        | 2016   |

    # Sail applies the first row's value to every row: Sail returns ['2016', '2016'].
    @column_args @sail-bug
    Scenario: date_format takes argument 2 from a column containing NULL
      When query
        """
        SELECT date_format('2016-04-08', c) AS result FROM VALUES (1, 'y'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2016   |
        | NULL   |

    @column_args @sail-bug
    Scenario: date_format takes argument 2 from a column holding two different values
      When query
        """
        SELECT date_format(TIMESTAMP '2026-02-02 10:20:30', c) AS result FROM VALUES (1, 'y'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 2026   |
        | 02     |
