@is_variant_null
Feature: is_variant_null output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to is_variant_null yields the schema Spark declares
      When query
        """
        SELECT is_variant_null(parse_json('null')) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to is_variant_null stays nullable
      When query
        """
        SELECT is_variant_null(c) AS result FROM VALUES (parse_json('null')), (CAST(NULL AS VARIANT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
