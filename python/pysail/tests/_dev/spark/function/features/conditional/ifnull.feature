Feature: ifnull output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to ifnull yields the schema Spark declares
      When query
        """
        SELECT ifnull(NULL, array('2')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

  Rule: The result has the common type of the two arguments, as with coalesce

    Scenario: ifnull widens a DECIMAL column and an integer to a DECIMAL
      When query
        """
        SELECT ifnull(d, 0) AS result, typeof(ifnull(d, 0)) AS result_type
        FROM VALUES (CAST(1.10 AS DECIMAL(10,2))), (NULL) AS t(d)
        """
      Then query result
        | result | result_type   |
        | 1.10   | decimal(12,2) |
        | 0.00   | decimal(12,2) |
