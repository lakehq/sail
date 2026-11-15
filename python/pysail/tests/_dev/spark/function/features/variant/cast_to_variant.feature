@spark-4
Feature: CAST(... AS VARIANT) output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: casting a non-null literal to variant yields a non-nullable variant
      When query
        """
        SELECT CAST(5 AS VARIANT) AS result
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = false)
        """

    @sail-bug
    Scenario: casting a non-null column to variant yields a non-nullable variant
      When query
        """
        SELECT CAST(id AS VARIANT) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = false)
        """

    Scenario: casting a nullable column to variant stays nullable
      When query
        """
        SELECT CAST(c AS VARIANT) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: variant (nullable = true)
        """
