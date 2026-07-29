@locate
Feature: locate output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to locate yields the schema Spark declares
      When query
        """
        SELECT locate('bar', 'foobarbar') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to locate yields the schema Spark declares
      When query
        """
        SELECT locate(CAST(id AS STRING), 'foobarbar') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to locate stays nullable
      When query
        """
        SELECT locate(c, 'foobarbar') AS result FROM VALUES ('bar'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
