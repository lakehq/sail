@slice
Feature: slice with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: slice — the argument may come from a column

    @column_args
    Scenario: slice with the argument as a literal
      When query
        """
        SELECT slice(array(1, 2, 3, 4), 2, 2) AS result
        """
      Then query result ordered
        | result |
        | [2, 3] |

    # Sail rejects the column: Sail errors: Invalid argument error: Non-nullable field of ListArray "item" cannot contain nulls
    @column_args @sail-bug
    Scenario Outline: slice takes argument <n> from a column containing NULL
      When query
        """
        SELECT slice(array(1, 2, 3, 4), <args>) AS result FROM VALUES (1, 2), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [2, 3] |
        | NULL   |

      Examples:
        | n | args |
        | 2 | c, 2 |
        | 3 | 2, c |

    @column_args
    Scenario: slice takes argument 2 from a column holding two different values
      When query
        """
        SELECT slice(array(1, 2, 3, 4), c, 2) AS result FROM VALUES (1, 1), (2, 3) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | [1, 2] |
        | [3, 4] |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null array literal yields a non-nullable array
      When query
        """
        SELECT slice(array(1, 2, 3), 1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null array column yields a non-nullable array
      When query
        """
        SELECT slice(array(id, id, id), 1, 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: long (containsNull = false)
        """

    Scenario: a nullable array column stays nullable
      When query
        """
        SELECT slice(c, 1, 2) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """

    @sail-bug
    Scenario: nullable input elements propagate to the element nullability
      When query
        """
        SELECT slice(c, 1, 2) AS result FROM VALUES (array(1, CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
