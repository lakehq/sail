Feature: aes_decrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: aes_decrypt — the argument may come from a column

    @column_args
    Scenario: aes_decrypt with the argument as a literal
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), '1234567890abcdef', 'CBC')) AS result
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |

    # Sail rejects the column: Sail errors: Spark `aes_decrypt`: Key requires a single value, got StringArray [ "1234567890abcdef", "1...
    @column_args @sail-bug
    Scenario: aes_decrypt takes argument 2 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), c, 'CBC')) AS result FROM VALUES (1, '1234567890abcdef'), (2, '1234567890abcdef') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |
        | 41706163686520537061726B |

    # Sail rejects the column: Sail errors: Spark `aes_decrypt`: Mode requires a single value, got StringArray [ "CBC", "CBC", ]
    @column_args @sail-bug
    Scenario: aes_decrypt takes argument 3 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), '1234567890abcdef', c)) AS result FROM VALUES (1, 'CBC'), (2, 'CBC') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |
        | 41706163686520537061726B |

    # Sail rejects the column: Sail errors: Spark `aes_decrypt`: AAD requires a single value, got StringArray [ "This is an AAD mixed...
    @column_args @sail-bug
    Scenario: aes_decrypt takes argument 5 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4'), 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', c)) AS result FROM VALUES (1, 'This is an AAD mixed into the input'), (2, 'This is an AAD mixed into the input') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 537061726B |
        | 537061726B |
