@power
Feature: power output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to power yields the schema Spark declares
      When query
        """
        SELECT power(2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to power yields the schema Spark declares
      When query
        """
        SELECT power(CAST(id AS INT), 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    Scenario: a nullable column input to power stays nullable
      When query
        """
        SELECT power(c, 3) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
