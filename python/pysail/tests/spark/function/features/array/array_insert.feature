@array_insert
Feature: array_insert with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: array_insert — the argument may come from a column

    @column_args
    Scenario: array_insert with the argument as a literal
      When query
        """
        SELECT array_insert(array(1, 2, 3, 4), 5, 5) AS result
        """
      Then query result ordered
        | result          |
        | [1, 2, 3, 4, 5] |

    # Sail rejects the column: Sail errors: array_insert: the index 0 is invalid. An index shall be either < 0 or > 0 (the first eleme...
    @column_args @sail-bug
    Scenario: array_insert takes argument 2 from a column containing NULL
      When query
        """
        SELECT array_insert(array(1, 2, 3, 4), c, 5) AS result FROM VALUES (1, 5), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result          |
        | [1, 2, 3, 4, 5] |
        | NULL            |

    @column_args
    Scenario: array_insert takes argument 2 from a column holding two different values
      When query
        """
        SELECT array_insert(array(1, 2, 3), c, 9) AS result FROM VALUES (1, 1), (2, 3) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result       |
        | [9, 1, 2, 3] |
        | [1, 2, 9, 3] |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null array literal yields a non-nullable array
      When query
        """
        SELECT array_insert(array(1, 2), 1, 9) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    @sail-bug
    Scenario: a non-null array column yields a non-nullable array
      When query
        """
        SELECT array_insert(array(id), 1, 9) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: long (containsNull = true)
        """

    Scenario: a nullable array column stays nullable
      When query
        """
        SELECT array_insert(c, 1, 9) AS result FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
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
        SELECT array_insert(c, 1, 9) AS result FROM VALUES (array(1, CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
