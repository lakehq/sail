Feature: arrays_overlap output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to arrays_overlap yields the schema Spark declares
      When query
        """
        SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5)) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable column input to arrays_overlap stays nullable
      When query
        """
        SELECT arrays_overlap(c, array(3, 4, 5)) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
