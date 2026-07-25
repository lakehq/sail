@log
Feature: log output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to log yields the schema Spark declares
      When query
        """
        SELECT log(10, 100) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to log yields the schema Spark declares
      When query
        """
        SELECT log(CAST(id AS INT), 100) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to log stays nullable
      When query
        """
        SELECT log(c, 100) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
