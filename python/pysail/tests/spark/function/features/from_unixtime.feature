Feature: from_unixtime with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: from_unixtime — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: from_unixtime with the argument as a literal
      When query
        """
        SELECT from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |

    # Sail applies the first row's value to every row: Sail returns ['1970-01-01 01:00:00', '1970-01-01 01:00:00'].
    @column_args @sail-bug
    Scenario: from_unixtime takes argument 2 from a column containing NULL
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy-MM-dd HH:mm:ss'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 1970-01-01 00:00:00 |
        | NULL                |

    @column_args @sail-bug
    Scenario: from_unixtime takes argument 2 from a column holding two different values
      When query
        """
        SELECT from_unixtime(0, c) AS result FROM VALUES (1, 'yyyy'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 1970   |
        | 01     |
