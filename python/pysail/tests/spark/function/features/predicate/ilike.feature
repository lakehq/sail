Feature: ilike output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to ilike yields the schema Spark declares
      When query
        """
        SELECT ilike('Spark', '_Park') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to ilike yields the schema Spark declares
      When query
        """
        SELECT ilike(CAST(id AS STRING), '_Park') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to ilike stays nullable
      When query
        """
        SELECT ilike(c, '_Park') AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
