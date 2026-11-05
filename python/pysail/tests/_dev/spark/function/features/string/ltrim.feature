Feature: ltrim output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to ltrim yields the schema Spark declares
      When query
        """
        SELECT ltrim('    SparkSQL   ') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to ltrim yields the schema Spark declares
      When query
        """
        SELECT ltrim(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to ltrim stays nullable
      When query
        """
        SELECT ltrim(c) AS result FROM VALUES ('    SparkSQL   '), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
