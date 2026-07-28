@substr
Feature: substr output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to substr yields the schema Spark declares
      When query
        """
        SELECT substr('Spark SQL', 5) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to substr yields the schema Spark declares
      When query
        """
        SELECT substr(CAST(id AS STRING), 5) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to substr stays nullable
      When query
        """
        SELECT substr(c, 5) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
