@map_from_entries
Feature: map_from_entries output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to map_from_entries yields the schema Spark declares
      When query
        """
        SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b'))) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = false)
        """

