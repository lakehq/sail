Feature: cbrt output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to cbrt yields the schema Spark declares
      When query
        """
        SELECT cbrt(27.0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to cbrt stays nullable
      When query
        """
        SELECT cbrt(c) AS result FROM VALUES (27.0), (CAST(NULL AS DECIMAL(3,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
