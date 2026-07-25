@try_divide
Feature: try_divide output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(3, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to try_divide yields the schema Spark declares
      When query
        """
        SELECT try_divide(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to try_divide stays nullable
      When query
        """
        SELECT try_divide(c, 2) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
