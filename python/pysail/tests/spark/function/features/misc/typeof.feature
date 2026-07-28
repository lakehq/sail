@typeof
Feature: typeof output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(1) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to typeof yields the schema Spark declares
      When query
        """
        SELECT typeof(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to typeof stays nullable
      When query
        """
        SELECT typeof(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
