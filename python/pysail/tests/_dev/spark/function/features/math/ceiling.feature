Feature: ceiling output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to ceiling yields the schema Spark declares
      When query
        """
        SELECT ceiling(-0.1) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """

    Scenario: a nullable column input to ceiling stays nullable
      When query
        """
        SELECT ceiling(c) AS result FROM VALUES (-0.1), (CAST(NULL AS DECIMAL(1,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(1,0) (nullable = true)
        """
