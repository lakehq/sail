Feature: like output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to like yields the schema Spark declares
      When query
        """
        SELECT like('Spark', '_park') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to like yields the schema Spark declares
      When query
        """
        SELECT like(CAST(id AS STRING), '_park') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to like stays nullable
      When query
        """
        SELECT like(c, '_park') AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
