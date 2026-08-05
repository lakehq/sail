Feature: replace output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to replace yields the schema Spark declares
      When query
        """
        SELECT replace('ABCabc', 'abc', 'DEF') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to replace yields the schema Spark declares
      When query
        """
        SELECT replace(CAST(id AS STRING), 'abc', 'DEF') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to replace stays nullable
      When query
        """
        SELECT replace(c, 'abc', 'DEF') AS result FROM VALUES ('ABCabc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
