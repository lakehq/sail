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

    Scenario Outline: DIV coerces every CASE branch before widening with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case
        """
      Then query result
        | dividend_case | divisor_case |
        | 3             | 6            |
      When query
        """
        SELECT id,
               (CASE WHEN id = 0 THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN id = 0 THEN 1 ELSE 0.5 END) AS divisor_case
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | dividend_case | divisor_case |
        | 0  | 0             | 3            |
        | 1  | 3             | 6            |

      Examples:
        | ansi  |
        | true  |
        | false |
