@left
Feature: left output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to left yields the schema Spark declares
      When query
        """
        SELECT left('Spark SQL', 3) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to left yields the schema Spark declares
      When query
        """
        SELECT left(CAST(id AS STRING), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to left stays nullable
      When query
        """
        SELECT left(c, 3) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
