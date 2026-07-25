@repeat
Feature: repeat output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to repeat yields the schema Spark declares
      When query
        """
        SELECT repeat('123', 2) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to repeat yields the schema Spark declares
      When query
        """
        SELECT repeat(CAST(id AS STRING), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to repeat stays nullable
      When query
        """
        SELECT repeat(c, 2) AS result FROM VALUES ('123'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
