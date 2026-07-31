@array_position
Feature: array_position output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_position yields the schema Spark declares
      When query
        """
        SELECT array_position(array(312, 773, 708, 708), 708) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column input to array_position stays nullable
      When query
        """
        SELECT array_position(c, 708) AS result FROM VALUES (array(312, 773, 708, 708)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
