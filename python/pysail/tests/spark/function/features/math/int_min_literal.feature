Feature: the INT minimum written as a literal

  Rule: -2147483648 is an INT literal, and DIV and ~ answer it

    # `number: MINUS? INTEGER_VALUE` (`SqlBaseParser.g4:1773`) makes `-2147483648` an INT.
    # `IntegralDivide.inputType` is `LongType` (`arithmetic.scala:890-893`), so the INT is widened
    # before dividing; `BitwiseNot` is `~` on the INT (`bitwiseExpressions.scala:184-201`).
    Scenario Outline: <expression> is <expected> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | expression                         | ansi  | expected   |
        | -2147483648 DIV -1                 | false | 2147483648 |
        | -2147483648 DIV -1                 | true  | 2147483648 |
        | CAST(-2147483648 AS INT) DIV -1    | true  | 2147483648 |
        | ~-2147483648                       | false | 2147483647 |
        | ~CAST(-2147483648 AS INT)          | true  | 2147483647 |
        | typeof(~CAST(5 AS SMALLINT))       | false | smallint   |
