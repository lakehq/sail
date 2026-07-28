@array
Feature: array output schema

  @spark_null
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
