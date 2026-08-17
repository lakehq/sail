Feature: size output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to size yields the schema Spark declares
      When query
        """
        SELECT size(array('b', 'd', 'c', 'a')) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to size stays nullable
      When query
        """
        SELECT size(c) AS result FROM VALUES (array('b', 'd', 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
