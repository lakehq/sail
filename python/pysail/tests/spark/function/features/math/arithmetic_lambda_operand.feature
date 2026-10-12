Feature: arithmetic operands inside lambda bodies vs Spark 4.2.0

  # A lambda variable is typed by the array or map it iterates, so the operand rules of `+ - * / %`
  # apply to it exactly as to a column: string promotion follows the mode, a DATE takes only an INT
  # offset, and a pair Spark refuses is refused inside `transform`, `filter` or `aggregate` too.
  # Measured on the JVM over 2568 lambda cells (`zz_david/lambda_review`): every higher-order
  # function Sail implements agrees on the verdict.

  Rule: a lambda variable is an operand of the element type

    Scenario Outline: <expression> is <type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expression>) AS t
        """
      Then query result
        | t      |
        | <type> |

      Examples:
        | ansi  | expression                                                               | type                            |
        | false | transform(array('4', '6'), x -> x + CAST(2 AS INT))                      | array<double>                   |
        | true  | transform(array('4', '6'), x -> x + CAST(2 AS INT))                      | array<bigint>                   |
        | false | transform(array('4', '6'), x -> -x)                                      | array<double>                   |
        | false | transform(array('4', '6'), x -> x * '2')                                 | array<double>                   |
        | false | transform(array(DATE'2024-01-15'), x -> x + CAST(2 AS INT))              | array<date>                     |
        | false | transform(array(DATE'2024-01-15'), (x, i) -> x + i)                      | array<date>                     |
        | false | transform(array(CAST(4 AS INT)), x -> x * INTERVAL '1' HOUR)             | array<interval day to second>   |
        | false | transform(array(TIMESTAMP'2024-01-15 12:00:00'), x -> x - DATE'2024-01-01') | array<interval day to second> |
        | true  | transform(array(CAST(NULL AS INT), 2), x -> x + NULL)                    | array<int>                      |
        | true  | exists(array('4', '6'), x -> (x / 2) IS NOT NULL)                        | boolean                         |
        | false | aggregate(array(CAST(4 AS INT), CAST(6 AS INT)), CAST(0 AS INT), (acc, x) -> acc + x) | int                |

  Rule: a pair the operator refuses is refused inside a lambda

    Scenario Outline: <expression> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | ansi  | expression                                                          |
        | false | transform(array(DATE'2024-01-15'), x -> x + CAST(2 AS BIGINT))      |
        | false | transform(array(DATE'2024-01-15'), x -> x * CAST(2 AS INT))         |
        | true  | transform(array('4', '6'), x -> x * '2')                            |
        | false | filter(array(DATE'2024-01-15'), x -> (x % 2) IS NOT NULL)           |
        | true  | transform(array(true), x -> -x)                                     |

  Rule: the lambda's value follows the operator

    Scenario: aggregate adds the elements
      When query
        """
        SELECT aggregate(array(CAST(4 AS INT), CAST(6 AS INT)), CAST(0 AS INT), (acc, x) -> acc + x) AS v
        """
      Then query result
        | v  |
        | 10 |
