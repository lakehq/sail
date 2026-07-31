@dayofyear
Feature: dayofyear output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to dayofyear yields the schema Spark declares
      When query
        """
        SELECT dayofyear('2016-04-09') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to dayofyear yields the schema Spark declares
      When query
        """
        SELECT dayofyear(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to dayofyear stays nullable
      When query
        """
        SELECT dayofyear(c) AS result FROM VALUES ('2016-04-09'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
