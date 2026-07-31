@predicate_in
Feature: in output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to in yields the schema Spark declares
      When query
        """
        SELECT 1 in(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: in through a force-nullable implicit cast: <case>
      When query
        """
        SELECT 1 IN (<input>, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

      Examples:
        | case             | input |
        | STRING -> BIGINT | '1'   |

    Scenario Outline: in without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT 1 IN (<input>, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |
