@try_mod
Feature: try_mod output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_mod yields the schema Spark declares
      When query
        """
        SELECT try_mod(3, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_mod yields the schema Spark declares
      When query
        """
        SELECT try_mod(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_mod stays nullable
      When query
        """
        SELECT try_mod(c, 2) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
