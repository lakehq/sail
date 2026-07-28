@expm1
Feature: expm1 output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to expm1 yields the schema Spark declares
      When query
        """
        SELECT expm1(0) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to expm1 yields the schema Spark declares
      When query
        """
        SELECT expm1(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to expm1 stays nullable
      When query
        """
        SELECT expm1(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
