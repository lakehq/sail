@split
Feature: split output schema

  @spark_null
  Rule: Output schema

    @sail-bug
Scenario: a non-null literal input to split yields the schema Spark declares
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a non-null column input to split yields the schema Spark declares
      When query
        """
        SELECT split(CAST(id AS STRING), '[ABC]') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a nullable column input to split stays nullable
      When query
        """
        SELECT split(c, '[ABC]') AS result FROM VALUES ('oneAtwoBthreeC'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = false)
        """
