Feature: div output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to div yields the schema Spark declares
      When query
        """
        SELECT 3 div 2 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: Integral division widens operands before computing the quotient

    Scenario Outline: INT minimum divided by negative one fits BIGINT with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -2147483648 DIV -1 AS result
        """
      Then query result
        | result     |
        | 2147483648 |
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

      Examples:
        | ansi  |
        | true  |
        | false |
