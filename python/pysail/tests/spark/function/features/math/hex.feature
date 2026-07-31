@hex
Feature: hex output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(17) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to hex stays nullable
      When query
        """
        SELECT hex(c) AS result FROM VALUES (17), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: hex without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT hex(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 17    |

    Scenario Outline: hex through a force-nullable implicit cast: <case>
      When query
        """
        SELECT hex(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | case             | input              |
        | DOUBLE -> BIGINT | CAST(17 AS DOUBLE) |
