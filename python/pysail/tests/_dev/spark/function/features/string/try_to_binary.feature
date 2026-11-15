Feature: try_to_binary with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: try_to_binary — the argument is resolved per row, not taken from the first row

    @function(columnargs)
    Scenario: try_to_binary with the argument as a literal
      When query
        """
        SELECT hex(try_to_binary('abc', 'utf-8')) AS result
        """
      Then query result ordered
        | result |
        | 616263 |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @function(columnargs) @sail-bug
    Scenario: try_to_binary takes argument 1 from a column holding two different values
      When query
        """
        select hex(try_to_binary(c, 'base64')) AS result FROM VALUES (1, 'a!'), (2, 'abc') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | NULL   |
        | 69B7   |

  Rule: try_to_binary — the argument must be foldable

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns NULL for every row.
    @function(columnargs) @sail-bug
    Scenario: try_to_binary takes argument 2 from a column holding two different values
      When query
        """
        select try_to_binary('a!', c) AS result FROM VALUES (1, 'base64'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns NULL for every row.
    @function(columnargs) @sail-bug
    Scenario: try_to_binary takes argument 2 from a column containing NULL
      When query
        """
        SELECT try_to_binary('abc', c) AS result FROM VALUES (1, 'utf-8'), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns NULL for every row.
    @function(columnargs) @sail-bug
    Scenario: try_to_binary takes argument 2 from a column
      When query
        """
        SELECT try_to_binary('abc', c) AS result FROM VALUES (1, 'utf-8'), (2, 'utf-8') AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to try_to_binary yields the schema Spark declares
      When query
        """
        SELECT try_to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to try_to_binary yields the schema Spark declares
      When query
        """
        SELECT try_to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to try_to_binary stays nullable
      When query
        """
        SELECT try_to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
