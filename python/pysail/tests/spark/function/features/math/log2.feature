@log2
Feature: log2 output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to log2 yields the schema Spark declares
      When query
        """
        SELECT log2(2) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to log2 yields the schema Spark declares
      When query
        """
        SELECT log2(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to log2 stays nullable
      When query
        """
        SELECT log2(c) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
