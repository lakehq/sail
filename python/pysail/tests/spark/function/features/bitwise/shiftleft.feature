Feature: shiftleft output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftleft stays nullable
      When query
        """
        SELECT shiftleft(c, 1) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: shiftleft through a force-nullable implicit cast: <case>
      When query
        """
        SELECT shiftleft(<input>, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

      Examples:
        | case          | input             |
        | DOUBLE -> INT | CAST(2 AS DOUBLE) |

    Scenario Outline: shiftleft without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT shiftleft(<input>, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 2     |
