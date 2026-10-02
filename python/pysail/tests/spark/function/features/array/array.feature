Feature: array output schema

  Rule: Void columns preserve rows

    Scenario Outline: Void columns in array(<arguments>)
      When query
        """
        SELECT array(<arguments>) AS result FROM VALUES (NULL), (NULL) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: void (containsNull = true)
        """
      Then query result
        | result   |
        | <result> |
        | <result> |

      Examples:
        | arguments | result       |
        | v         | [NULL]       |
        | v, v      | [NULL, NULL] |
        | v, NULL   | [NULL, NULL] |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to array yields the schema Spark declares
      When query
        """
        SELECT array(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a non-null column input to array yields the schema Spark declares
      When query
        """
        SELECT array(CAST(id AS INT), 2, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to array stays nullable
      When query
        """
        SELECT array(c, 2, 3) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

    # Spark's `Coalesce.nullable` is `children.forall(_.nullable)`, so a non-nullable branch keeps
    # the whole coalesce non-nullable even if another branch is a forced-nullable cast.
    Scenario: a forced-nullable cast inside a coalesce with a non-nullable branch stays non-nullable
      When query
        """
        SELECT array(coalesce(CAST('1' AS INT), 2)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a forced-nullable cast inside a coalesce where every branch is nullable stays nullable
      When query
        """
        SELECT array(coalesce(CAST('1' AS INT), CAST(NULL AS INT))) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
