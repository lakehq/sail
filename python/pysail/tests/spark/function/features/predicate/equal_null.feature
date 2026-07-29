@equal_null
Feature: equal_null output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to equal_null yields the schema Spark declares
      When query
        """
        SELECT equal_null(3, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a non-null column input to equal_null yields the schema Spark declares
      When query
        """
        SELECT equal_null(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to equal_null stays nullable
      When query
        """
        SELECT equal_null(c, 3) AS result FROM VALUES (3), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
