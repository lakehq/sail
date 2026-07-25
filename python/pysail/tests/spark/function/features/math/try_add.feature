@try_add
Feature: try_add output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to try_add yields the schema Spark declares
      When query
        """
        SELECT try_add(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to try_add stays nullable
      When query
        """
        SELECT try_add(c, 2) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
