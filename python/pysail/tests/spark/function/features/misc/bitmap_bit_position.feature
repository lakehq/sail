@bitmap_bit_position
Feature: bitmap_bit_position output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to bitmap_bit_position yields the schema Spark declares
      When query
        """
        SELECT bitmap_bit_position(1) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to bitmap_bit_position yields the schema Spark declares
      When query
        """
        SELECT bitmap_bit_position(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to bitmap_bit_position stays nullable
      When query
        """
        SELECT bitmap_bit_position(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  @spark_null
  Rule: Nullability through Spark's implicit casts
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: bitmap_bit_position loses non-nullability through Spark's implicit cast: <case>
      When query
        """
        SELECT bitmap_bit_position(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = <nullable>)
        """

      Examples:
        | case             | input             | nullable |
        | no cast          | 1                 | false    |
        | DOUBLE -> BIGINT | CAST(1 AS DOUBLE) | true     |
