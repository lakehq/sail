Feature: conv with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: conv — the argument may come from a column

    @function(columnargs)
    Scenario: conv with the argument as a literal
      When query
        """
        SELECT conv('100', 2, 10) AS result
        """
      Then query result ordered
        | result |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs) @sail-bug
    Scenario: conv takes argument 2 from a column holding two different values
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 16) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 256    |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs) @sail-bug
    Scenario: conv takes argument 2 from a column
      When query
        """
        SELECT conv('100', c, 10) AS result FROM VALUES (1, 2), (2, 2) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |

    # Sail rejects the column: Sail errors: Unsupported Data Type: Spark `spark_conv` function expects (Utf8 | Utf8View | LargeUtf8 |...
    @function(columnargs) @sail-bug
    Scenario: conv takes argument 3 from a column
      When query
        """
        SELECT conv('100', 2, c) AS result FROM VALUES (1, 10), (2, 10) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 4      |
        | 4      |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal is nullable (conv is inherently nullable in Spark)
      When query
        """
        SELECT conv('11', 2, 10) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT conv(c, 2, 10) AS result FROM VALUES ('11'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null string column is nullable (conv is inherently nullable in Spark)
      When query
        """
        SELECT conv(CAST(id AS STRING), 10, 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_conv.txt doctests)

    Scenario: conv doctest #1 — to_binary/octal/hex
      When query
        """
        SELECT conv('10', 10, 2) as to_binary, conv('10', 10, 8) as to_octal, conv('10', 10, 16) as to_hex
        """
      Then query result
        | to_binary | to_octal | to_hex |
        | 1010      | 12       | A      |
