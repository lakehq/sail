@try_multiply
Feature: try_multiply output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_multiply yields the schema Spark declares
      When query
        """
        SELECT try_multiply(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_multiply stays nullable
      When query
        """
        SELECT try_multiply(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
