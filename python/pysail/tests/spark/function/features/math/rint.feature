Feature: rint output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to rint yields the schema Spark declares
      When query
        """
        SELECT rint(12.3456) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to rint stays nullable
      When query
        """
        SELECT rint(c) AS result FROM VALUES (12.3456), (CAST(NULL AS DECIMAL(6,4))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
