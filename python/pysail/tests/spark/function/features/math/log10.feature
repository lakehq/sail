Feature: log10 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to log10 yields the schema Spark declares
      When query
        """
        SELECT log10(10) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to log10 yields the schema Spark declares
      When query
        """
        SELECT log10(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to log10 stays nullable
      When query
        """
        SELECT log10(c) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
