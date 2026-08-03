@weekday
Feature: weekday output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to weekday yields the schema Spark declares
      When query
        """
        SELECT weekday('2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to weekday yields the schema Spark declares
      When query
        """
        SELECT weekday(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to weekday stays nullable
      When query
        """
        SELECT weekday(c) AS result FROM VALUES ('2009-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
