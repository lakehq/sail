Feature: sqrt output schema

  Rule: Result values

    Scenario: negative double input returns NaN
      When query
        """
        SELECT isnan(sqrt(CAST(-1 AS DOUBLE))) AS result
        """
      Then query result
        | result |
        | true   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to sqrt yields the schema Spark declares
      When query
        """
        SELECT sqrt(4) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to sqrt yields the schema Spark declares
      When query
        """
        SELECT sqrt(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to sqrt stays nullable
      When query
        """
        SELECT sqrt(c) AS result FROM VALUES (4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
