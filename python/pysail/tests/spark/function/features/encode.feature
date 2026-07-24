Feature: encode with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: encode — the argument may come from a column

    @column_args
    Scenario: encode with the argument as a literal
      When query
        """
        SELECT hex(encode('abc', 'utf-8')) AS result
        """
      Then query result ordered
        | result |
        | 616263 |

    # Sail rejects the column: Sail errors: Unsupported args [Scalar(Utf8("abc")), Array(StringArray [ "utf-8", null, ])] for Spark fu...
    @column_args @sail-bug
    Scenario: encode takes argument 2 from a column containing NULL
      When query
        """
        SELECT hex(encode('abc', c)) AS result FROM VALUES (1, 'utf-8'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 616263 |
        | NULL   |

    # Sail rejects the column: Sail errors: Unsupported args [Scalar(Utf8("abc")), Array(StringArray [ "utf-8", "utf-8", ])] for Spark...
    @column_args @sail-bug
    Scenario: encode takes argument 2 from a column
      When query
        """
        SELECT hex(encode('abc', c)) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 616263 |
        | 616263 |

    @column_args @sail-bug
    Scenario: encode takes argument 2 from a column holding two different values
      When query
        """
        SELECT hex(encode('ab', c)) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-16') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result       |
        | 6162         |
        | FEFF00610062 |
