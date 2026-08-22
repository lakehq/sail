Feature: assert_true output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to assert_true yields the schema Spark declares
      When query
        """
        SELECT assert_true(0 < 1) AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    Scenario: a nullable column input to assert_true stays nullable
      When query
        """
        SELECT assert_true(c) AS result FROM VALUES (0 < 1), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """
