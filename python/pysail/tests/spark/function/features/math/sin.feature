Feature: sin output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to sin yields the schema Spark declares
      When query
        """
        SELECT sin(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to sin yields the schema Spark declares
      When query
        """
        SELECT sin(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to sin stays nullable
      When query
        """
        SELECT sin(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
