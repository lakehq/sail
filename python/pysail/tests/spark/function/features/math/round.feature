Feature: round with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: round — the argument must be foldable

    @function(columnargs)
    Scenario: round with the argument as a literal
      When query
        """
        SELECT round(2.5, 0) AS result
        """
      Then query result ordered
        | result |
        | 3      |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', 'NULL'].
    @function(columnargs) @sail-bug
    Scenario: round takes argument 2 from a column containing NULL
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', '3.0'].
    @function(columnargs) @sail-bug
    Scenario: round takes argument 2 from a column
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, 0) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null numeric literal is nullable (inherently nullable in Spark)
      When query
        """
        SELECT round(2.567, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(4,2) (nullable = true)
        """

    @sail-bug
    Scenario: a non-null numeric column is nullable (inherently nullable in Spark)
      When query
        """
        SELECT round(id, 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable numeric column stays nullable
      When query
        """
        SELECT round(c, 1) AS result FROM VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
