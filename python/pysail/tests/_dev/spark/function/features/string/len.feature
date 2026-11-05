Feature: len output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to len yields the schema Spark declares
      When query
        """
        SELECT len('Spark SQL ') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to len yields the schema Spark declares
      When query
        """
        SELECT len(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to len stays nullable
      When query
        """
        SELECT len(c) AS result FROM VALUES ('Spark SQL '), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
