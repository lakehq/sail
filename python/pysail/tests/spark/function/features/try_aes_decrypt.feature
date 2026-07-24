Feature: try_aes_decrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: try_aes_decrypt — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: try_aes_decrypt with the argument as a literal
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM')) AS result
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario: try_aes_decrypt takes argument 2 from a column containing NULL
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), c, 'GCM')) AS result FROM VALUES (1, '0000111122223333'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |
        | NULL               |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario: try_aes_decrypt takes argument 2 from a column
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), c, 'GCM')) AS result FROM VALUES (1, '0000111122223333'), (2, '0000111122223333') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |
        | 537061726B2053514C |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario: try_aes_decrypt takes argument 3 from a column containing NULL
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', c)) AS result FROM VALUES (1, 'GCM'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |
        | NULL               |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario: try_aes_decrypt takes argument 3 from a column
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', c)) AS result FROM VALUES (1, 'GCM'), (2, 'GCM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |
        | 537061726B2053514C |
