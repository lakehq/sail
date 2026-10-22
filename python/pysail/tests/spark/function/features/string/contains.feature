Feature: contains output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to contains yields the schema Spark declares
      When query
        """
        SELECT contains('Spark SQL', 'Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to contains yields the schema Spark declares
      When query
        """
        SELECT contains(CAST(id AS STRING), 'Spark') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to contains stays nullable
      When query
        """
        SELECT contains(c, 'Spark') AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
