@datediff
Feature: datediff output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff(CAST(id AS STRING), '2009-07-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a nullable column input to datediff stays nullable
      When query
        """
        SELECT datediff(c, '2009-07-30') AS result FROM VALUES ('2009-07-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
