@shuffle
Feature: shuffle output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to shuffle yields the schema Spark declares
      When query
        """
        SELECT shuffle(array(1, 20, 3, 5)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to shuffle stays nullable
      When query
        """
        SELECT shuffle(c) AS result FROM VALUES (array(1, 20, 3, 5)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = true)
        """
