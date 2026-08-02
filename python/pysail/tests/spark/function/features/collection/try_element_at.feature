@try_element_at
Feature: try_element_at output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to try_element_at yields the schema Spark declares
      When query
        """
        SELECT try_element_at(array(1, 2, 3), 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to try_element_at stays nullable
      When query
        """
        SELECT try_element_at(c, 2) AS result FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
