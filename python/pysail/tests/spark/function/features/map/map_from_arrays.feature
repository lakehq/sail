@map_from_arrays
Feature: map_from_arrays output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to map_from_arrays yields the schema Spark declares
      When query
        """
        SELECT map_from_arrays(array(1.0, 3.0), array('2', '4')) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: a nullable column input to map_from_arrays stays nullable
      When query
        """
        SELECT map_from_arrays(c, array('2', '4')) AS result FROM VALUES (array(1.0, 3.0)), (CAST(NULL AS ARRAY<DECIMAL(2,1)>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """
