Feature: variant_get with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.1.1.

  Rule: variant_get — the argument may come from a column

    @column_args
    Scenario: variant_get with the argument as a literal
      When query
        """
        SELECT variant_get(parse_json('[1, "hello"]'), '$[1]') AS result
        """
      Then query result ordered
        | result  |
        | "hello" |

    # Sail rejects the column: Sail errors: Spark `variant_get` function: path must be a constant string
    @column_args @sail-bug
    Scenario: variant_get takes argument 2 from a column holding two different values
      When query
        """
        SELECT variant_get(parse_json('[1, "hello"]'), c) AS result FROM VALUES (1, '$[1]'), (2, '$.a') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result  |
        | "hello" |
        | NULL    |

    # Sail rejects the column: Sail errors: Spark `variant_get` function: path must be a constant string
    @column_args @sail-bug
    Scenario: variant_get takes argument 2 from a column
      When query
        """
        SELECT variant_get(parse_json('[1, "hello"]'), c) AS result FROM VALUES (1, '$[1]'), (2, '$[1]') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result  |
        | "hello" |
        | "hello" |
