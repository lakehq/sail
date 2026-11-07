Feature: lower output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to lower yields the schema Spark declares
      When query
        """
        SELECT lower('SparkSql') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to lower yields the schema Spark declares
      When query
        """
        SELECT lower(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to lower stays nullable
      When query
        """
        SELECT lower(c) AS result FROM VALUES ('SparkSql'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Binary input

    Scenario: lower replaces invalid UTF-8 in binary input
      When query
        """
        SELECT lower(X'C3416263') AS result
        """
      Then query result
        | result |
        | �abc   |
