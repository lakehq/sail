@array_join
Feature: array_join output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_join yields the schema Spark declares
      When query
        """
        SELECT array_join(array('hello', 'world'), ' ') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to array_join stays nullable
      When query
        """
        SELECT array_join(c, ' ') AS result FROM VALUES (array('hello', 'world')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
