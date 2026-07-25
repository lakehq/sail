@rtrim
Feature: rtrim output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to rtrim yields the schema Spark declares
      When query
        """
        SELECT rtrim('    SparkSQL   ') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to rtrim yields the schema Spark declares
      When query
        """
        SELECT rtrim(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to rtrim stays nullable
      When query
        """
        SELECT rtrim(c) AS result FROM VALUES ('    SparkSQL   '), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
