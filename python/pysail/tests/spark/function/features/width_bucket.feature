@width_bucket
Feature: width_bucket output schema

  @spark_null
  Rule: Output schema

    @sail-bug
Scenario: a non-null literal input to width_bucket yields the schema Spark declares
      When query
        """
        SELECT width_bucket(5.3, 0.2, 10.6, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    @sail-bug
Scenario: a nullable column input to width_bucket stays nullable
      When query
        """
        SELECT width_bucket(c, 0.2, 10.6, 5) AS result FROM VALUES (5.3), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
