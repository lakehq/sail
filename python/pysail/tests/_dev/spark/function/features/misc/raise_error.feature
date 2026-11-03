Feature: raise_error output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to raise_error yields the schema Spark declares
      When query
        """
        SELECT raise_error('custom error message') AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a non-null column input to raise_error yields the schema Spark declares
      When query
        """
        SELECT raise_error(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a nullable column input to raise_error stays nullable
      When query
        """
        SELECT raise_error(c) AS result FROM VALUES ('custom error message'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """
