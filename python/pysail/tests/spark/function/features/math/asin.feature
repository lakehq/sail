@asin
Feature: asin output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to asin yields the schema Spark declares
      When query
        """
        SELECT asin(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to asin yields the schema Spark declares
      When query
        """
        SELECT asin(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to asin stays nullable
      When query
        """
        SELECT asin(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
