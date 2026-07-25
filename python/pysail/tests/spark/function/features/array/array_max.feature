@array_max
Feature: array_max output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to array_max yields the schema Spark declares
      When query
        """
        SELECT array_max(array(1, 20, null, 3)) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to array_max stays nullable
      When query
        """
        SELECT array_max(c) AS result FROM VALUES (array(1, 20, null, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
