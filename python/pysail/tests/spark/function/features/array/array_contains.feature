@array_contains
Feature: array_contains output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_contains yields the schema Spark declares
      When query
        """
        SELECT array_contains(array(1, 2, 3), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to array_contains stays nullable
      When query
        """
        SELECT array_contains(c, 2) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
