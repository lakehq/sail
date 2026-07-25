@map
Feature: map output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map yields the schema Spark declares
      When query
        """
        SELECT map(1.0, '2', 3.0, '4') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to map stays nullable
      When query
        """
        SELECT map(c, '2', 3.0, '4') AS result FROM VALUES (1.0), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """
