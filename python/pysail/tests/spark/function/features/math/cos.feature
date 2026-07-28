@cos
Feature: cos output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to cos yields the schema Spark declares
      When query
        """
        SELECT cos(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to cos yields the schema Spark declares
      When query
        """
        SELECT cos(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to cos stays nullable
      When query
        """
        SELECT cos(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
