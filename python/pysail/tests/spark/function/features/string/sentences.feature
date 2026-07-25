@sentences
Feature: sentences output schema

  @spark_null
  Rule: Output schema

    @sail-bug
Scenario: a non-null literal input to sentences yields the schema Spark declares
      When query
        """
        SELECT sentences('Hi there! Good morning.') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a non-null column input to sentences yields the schema Spark declares
      When query
        """
        SELECT sentences(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a nullable column input to sentences stays nullable
      When query
        """
        SELECT sentences(c) AS result FROM VALUES ('Hi there! Good morning.'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: array (containsNull = false)
         |    |    |-- element: string (containsNull = false)
        """
