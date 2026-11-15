Feature: day output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to day yields the schema Spark declares
      When query
        """
        SELECT day('2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to day yields the schema Spark declares
      When query
        """
        SELECT day(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to day stays nullable
      When query
        """
        SELECT day(c) AS result FROM VALUES ('2009-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
