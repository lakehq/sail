@nanvl
Feature: nanvl output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to nanvl yields the schema Spark declares
      When query
        """
        SELECT nanvl(cast('NaN' as double), 123) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to nanvl yields the schema Spark declares
      When query
        """
        SELECT nanvl(CAST(id AS DOUBLE), 123) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    Scenario: a nullable column input to nanvl stays nullable
      When query
        """
        SELECT nanvl(c, 123) AS result FROM VALUES (cast('NaN' as double)), (CAST(NULL AS DOUBLE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
