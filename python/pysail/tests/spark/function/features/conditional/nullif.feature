@nullif
Feature: nullif output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(2, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullif stays nullable
      When query
        """
        SELECT nullif(c, 2) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
