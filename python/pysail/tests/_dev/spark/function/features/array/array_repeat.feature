Feature: array_repeat output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_repeat yields the schema Spark declares
      When query
        """
        SELECT array_repeat('123', 2) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null column input to array_repeat yields the schema Spark declares
      When query
        """
        SELECT array_repeat(CAST(id AS STRING), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to array_repeat stays nullable
      When query
        """
        SELECT array_repeat(c, 2) AS result FROM VALUES ('123'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """
