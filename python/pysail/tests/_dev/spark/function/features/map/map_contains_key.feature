Feature: map_contains_key output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to map_contains_key yields the schema Spark declares
      When query
        """
        SELECT map_contains_key(map(1, 'a', 2, 'b'), 1) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a nullable column input to map_contains_key stays nullable
      When query
        """
        SELECT map_contains_key(c, 1) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
