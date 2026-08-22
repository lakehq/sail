Feature: encode with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: encode — the argument may come from a column

    @function(columnargs)
    Scenario: encode with the argument as a literal
      When query
        """
        SELECT hex(encode('abc', 'utf-8')) AS result
        """
      Then query result ordered
        | result |
        | 616263 |

    # Sail rejects the column: Sail errors: Unsupported args [Scalar(Utf8("abc")), Array(StringArray [ "utf-8", null, ])] for Spark fu...
    @function(columnargs) @sail-bug
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
    @function(columnargs) @sail-bug
    Scenario: encode takes argument 2 from a column
      When query
        """
        SELECT hex(encode('abc', c)) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | 616263 |
        | 616263 |

    @function(columnargs) @sail-bug
    Scenario: encode takes argument 2 from a column holding two different values
      When query
        """
        SELECT hex(encode('ab', c)) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-16') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result       |
        | 6162         |
        | FEFF00610062 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal is nullable (encode is inherently nullable in Spark)
      When query
        """
        SELECT encode('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT encode(c, 'utf-8') AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null string column is nullable (encode is inherently nullable in Spark)
      When query
        """
        SELECT encode(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

  Rule: Argument count validation

    # Guardrail, not a wording test: `return_field_from_args` runs before `coerce_types`, so
    # without an arity check there this indexed out of bounds and killed the connection
    # instead of raising. A dead RPC carries no message, so this regex fails on the panic.
    # Loose on purpose — it is the widest wording both engines share.
    Scenario: encode rejects a single argument
      When query
        """
        SELECT encode('a') AS result
        """
      Then query error `encode`.*requires 2

    # Spark reports arity through WRONG_NUM_ARGS; Sail emits its own wording. Systemic across
    # the whole function surface, so it is recorded rather than worked around here.
    @sail-bug
    Scenario: encode reports arity through WRONG_NUM_ARGS
      When query
        """
        SELECT encode('a') AS result
        """
      Then query error \[WRONG_NUM_ARGS.*The `encode` requires 2 parameters but the actual number is 1
