@sequence
Feature: sequence output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null column input to sequence yields the schema Spark declares
      When query
        """
        SELECT sequence(CAST(id AS INT), 5) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to sequence stays nullable
      When query
        """
        SELECT sequence(c, 5) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: sequence loses non-nullability through Spark's implicit cast: <case>
      When query
        """
        SELECT sequence(<input>, 5) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = <nullable>)
         |    |-- element: <element> (containsNull = false)
        """

      Examples:
        | case             | input | nullable | element |
        | no cast          | 1     | false    | integer |
        | STRING -> BIGINT | '1'   | true     | long    |
