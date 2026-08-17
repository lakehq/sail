Feature: ln output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to ln yields the schema Spark declares
      When query
        """
        SELECT ln(1) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to ln yields the schema Spark declares
      When query
        """
        SELECT ln(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to ln stays nullable
      When query
        """
        SELECT ln(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
