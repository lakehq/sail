Feature: variant_get with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: variant_get — the argument may come from a column

    @function(columnargs)
    Scenario: variant_get with the argument as a literal
      When query
        """
        SELECT variant_get(parse_json('[1, "hello"]'), '$[1]') AS result
        """
      Then query result ordered
        | result  |
        | "hello" |

    # Sail rejects the column: Sail errors: Spark `variant_get` function: path must be a constant string
    @function(columnargs) @sail-bug
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
    @function(columnargs) @sail-bug
    Scenario: variant_get takes argument 2 from a column
      When query
        """
        SELECT variant_get(parse_json('[1, "hello"]'), c) AS result FROM VALUES (1, '$[1]'), (2, '$[1]') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result  |
        | "hello" |
        | "hello" |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to variant_get yields the schema Spark declares
      When query
        """
        SELECT variant_get(parse_json('{"a": 1}'), '$.a', 'int') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to variant_get stays nullable
      When query
        """
        SELECT variant_get(c, '$.a', 'int') AS result FROM VALUES (parse_json('{"a": 1}')), (CAST(NULL AS VARIANT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
