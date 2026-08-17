Feature: endswith output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to endswith yields the schema Spark declares
      When query
        """
        SELECT endswith('Spark SQL', 'SQL') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to endswith yields the schema Spark declares
      When query
        """
        SELECT endswith(CAST(id AS STRING), 'SQL') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to endswith stays nullable
      When query
        """
        SELECT endswith(c, 'SQL') AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
