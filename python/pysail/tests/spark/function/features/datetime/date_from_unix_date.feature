@date_from_unix_date
Feature: date_from_unix_date output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a non-null column input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to date_from_unix_date stays nullable
      When query
        """
        SELECT date_from_unix_date(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: date_from_unix_date loses non-nullability through Spark's implicit cast: <case>
      When query
        """
        SELECT date_from_unix_date(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case          | input             |
        | STRING -> INT | '1'               |
        | DOUBLE -> INT | CAST(1 AS DOUBLE) |

    Scenario Outline: date_from_unix_date without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT date_from_unix_date(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |
