@md5
Feature: md5 output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to md5 yields the schema Spark declares
      When query
        """
        SELECT md5('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to md5 yields the schema Spark declares
      When query
        """
        SELECT md5(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to md5 stays nullable
      When query
        """
        SELECT md5(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
