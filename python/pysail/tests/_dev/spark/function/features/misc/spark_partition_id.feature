Feature: spark_partition_id output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to spark_partition_id yields the schema Spark declares
      When query
        """
        SELECT spark_partition_id() AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
