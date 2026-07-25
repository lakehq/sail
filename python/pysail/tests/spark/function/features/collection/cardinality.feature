@cardinality
Feature: cardinality output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to cardinality yields the schema Spark declares
      When query
        """
        SELECT cardinality(array('b', 'd', 'c', 'a')) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to cardinality stays nullable
      When query
        """
        SELECT cardinality(c) AS result FROM VALUES (array('b', 'd', 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
