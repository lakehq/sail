Feature: sort_array output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to sort_array yields the schema Spark declares
      When query
        """
        SELECT sort_array(array('b', 'd', null, 'c', 'a'), true) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to sort_array stays nullable
      When query
        """
        SELECT sort_array(c, true) AS result FROM VALUES (array('b', 'd', null, 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
