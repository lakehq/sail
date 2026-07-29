@log1p
Feature: log1p output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to log1p yields the schema Spark declares
      When query
        """
        SELECT log1p(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to log1p yields the schema Spark declares
      When query
        """
        SELECT log1p(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to log1p stays nullable
      When query
        """
        SELECT log1p(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
