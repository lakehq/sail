Feature: decode with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: decode — the argument may come from a column

    @function(columnargs)
    Scenario: decode with the argument as a literal
      When query
        """
        SELECT decode(encode('abc', 'utf-8'), 'utf-8') AS result
        """
      Then query result ordered
        | result |
        | abc    |

    # Sail rejects the column: Sail errors: Unsupported args [Scalar(Binary("97,98,99")), Array(StringArray [ "utf-8", null, ])] for S...
    @function(columnargs) @sail-bug
    Scenario: decode takes argument 2 from a column containing NULL
      When query
        """
        SELECT decode(encode('abc', 'utf-8'), c) AS result FROM VALUES (1, 'utf-8'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | abc    |
        | NULL   |

    # Sail rejects the column: Sail errors: Unsupported args [Scalar(Binary("97,98,99")), Array(StringArray [ "utf-8", "utf-8", ])] fo...
    @function(columnargs) @sail-bug
    Scenario: decode takes argument 2 from a column
      When query
        """
        SELECT decode(encode('abc', 'utf-8'), c) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | abc    |
        | abc    |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null binary literal is nullable (decode is inherently nullable in Spark)
      When query
        """
        SELECT decode(X'616263', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable binary column stays nullable
      When query
        """
        SELECT decode(c, 'utf-8') AS result FROM VALUES (X'61'), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null binary column is nullable (decode is inherently nullable in Spark)
      When query
        """
        SELECT decode(CAST(CAST(id AS STRING) AS BINARY), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
