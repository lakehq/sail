@floor
Feature: floor output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to floor yields the schema Spark declares
      When query
        """
        SELECT floor(-0.1) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """

    Scenario: a nullable column input to floor stays nullable
      When query
        """
        SELECT floor(c) AS result FROM VALUES (-0.1), (CAST(NULL AS DECIMAL(1,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """
