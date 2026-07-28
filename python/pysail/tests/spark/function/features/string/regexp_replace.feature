@regexp_replace
Feature: regexp_replace output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to regexp_replace yields the schema Spark declares
      When query
        """
        SELECT regexp_replace('100-200', '(\\d+)', 'num') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to regexp_replace yields the schema Spark declares
      When query
        """
        SELECT regexp_replace(CAST(id AS STRING), '(\\d+)', 'num') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to regexp_replace stays nullable
      When query
        """
        SELECT regexp_replace(c, '(\\d+)', 'num') AS result FROM VALUES ('100-200'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
