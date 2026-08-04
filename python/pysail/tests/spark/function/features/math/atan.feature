Feature: atan output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to atan yields the schema Spark declares
      When query
        """
        SELECT atan(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to atan yields the schema Spark declares
      When query
        """
        SELECT atan(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to atan stays nullable
      When query
        """
        SELECT atan(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
