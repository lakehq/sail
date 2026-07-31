@positive
Feature: positive output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to positive yields the schema Spark declares
      When query
        """
        SELECT positive(1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to positive yields the schema Spark declares
      When query
        """
        SELECT positive(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to positive stays nullable
      When query
        """
        SELECT positive(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: positive through a force-nullable implicit cast: <case>
      When query
        """
        SELECT positive(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

      Examples:
        | case             | input |
        | STRING -> DOUBLE | '1'   |

    Scenario Outline: positive without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT positive(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |
