Feature: dayname output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to dayname yields the schema Spark declares
      When query
        """
        SELECT dayname(DATE('2008-02-20')) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to dayname stays nullable
      When query
        """
        SELECT dayname(c) AS result FROM VALUES (DATE('2008-02-20')), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
