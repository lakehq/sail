@month
Feature: month output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to month yields the schema Spark declares
      When query
        """
        SELECT month('2016-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to month yields the schema Spark declares
      When query
        """
        SELECT month(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to month stays nullable
      When query
        """
        SELECT month(c) AS result FROM VALUES ('2016-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
