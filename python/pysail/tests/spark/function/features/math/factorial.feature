Feature: factorial output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(5) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a non-null column input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable column input to factorial stays nullable
      When query
        """
        SELECT factorial(c) AS result FROM VALUES (5), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
