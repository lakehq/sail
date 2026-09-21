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

  Rule: the result keeps the type of the value

    @sail-bug
    Scenario: round of an INT returns an INT
      When query
        """
        SELECT round(25, -1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
      Then query result
        | result |
        | 30     |

  Rule: a string value is cast to a double

    @sail-bug
    Scenario: round of a string
      When query
        """
        SELECT round('2.5') AS result
        """
      Then query result
        | result |
        | 3.0    |

  Rule: rounding that overflows the type errors under ANSI, and wraps otherwise

    Scenario: round overflowing an INT errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(2147483647, -1) AS result
        """
      Then query error (?i)overflow

    @sail-bug
    Scenario: round overflowing an INT wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT round(2147483647, -1) AS result
        """
      Then query result
        | result      |
        | -2147483646 |

  Rule: the value may come from a column

    # The literal scenarios above are constant-folded, so they never exercise the columnar kernel --
    # the path production actually takes. These repeat them with the value in a column.

    Scenario: round of an INT column keeps the INT type
      When query
        """
        SELECT round(v, -1) AS result FROM VALUES (1, 25), (2, 24), (3, CAST(NULL AS INT)) AS t(i, v) ORDER BY i
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
      Then query result ordered
        | result |
        | 30     |
        | 20     |
        | NULL   |

    @sail-bug
    Scenario: round of a string column
      When query
        """
        SELECT round(v) AS result FROM VALUES (1, '2.5'), (2, '3.5') AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result |
        | 3.0    |
        | 4.0    |

    Scenario: round of an INT column that overflows errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(v, -1) AS result FROM VALUES (1, 2147483647), (2, 1) AS t(i, v) ORDER BY i
        """
      Then query error (?i)overflow

  Rule: a negative scale widens the DECIMAL precision

    # Spark's rule is `max(p - s + 1, -scale + 1)`, so the type has room for the rounded magnitude.
    # The VALUE is the same either way -- only the schema shows it, which is why a values-only test
    # is blind to this.

    @sail-bug
    Scenario: round with a very negative scale keeps room for the magnitude
      When query
        """
        SELECT round(CAST(1.5 AS DECIMAL(3,0)), -5) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(6,0) (nullable = true)
        """

    @sail-bug
    Scenario: round with a negative scale beyond the precision caps at 38
      When query
        """
        SELECT round(CAST(1.5 AS DECIMAL(2,1)), -40) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(38,0) (nullable = true)
        """
