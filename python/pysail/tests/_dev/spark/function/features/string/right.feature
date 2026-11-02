Feature: right output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to right yields the schema Spark declares
      When query
        """
        SELECT right('Spark SQL', 3) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to right yields the schema Spark declares
      When query
        """
        SELECT right(CAST(id AS STRING), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to right stays nullable
      When query
        """
        SELECT right(c, 3) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
