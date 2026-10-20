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

  Rule: Narrow integers are promoted before division

    Scenario Outline: DIV promotes integer boundaries with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -2147483648 DIV -1 AS result,
               typeof(-2147483648 DIV -1) AS result_type,
               CAST(-128 AS TINYINT) DIV CAST(-1 AS TINYINT) AS tiny_result,
               CAST(-32768 AS SMALLINT) DIV CAST(-1 AS SMALLINT) AS small_result
        """
      Then query result
        | result     | result_type | tiny_result | small_result |
        | 2147483648 | bigint      | 128         | 32768        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: DIV promotes integer columns and preserves NULL with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, value DIV -1 AS result, typeof(value DIV -1) AS result_type
        FROM VALUES (0, -2147483648), (1, -2147483647), (2, 0),
                    (3, 2147483647), (4, CAST(NULL AS INT)) AS t(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result      | result_type |
        | 0  | 2147483648  | bigint      |
        | 1  | 2147483647  | bigint      |
        | 2  | 0           | bigint      |
        | 3  | -2147483647 | bigint      |
        | 4  | NULL        | bigint      |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Zero divisors preserve the BIGINT result type

    Scenario: Negative decimal zero DIV preserves BIGINT arithmetic outside ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(1 DIV -0.0BD) AS div_type,
               coalesce(1 DIV -0.0BD, 2147483647) + 1 AS result
        """
      Then query result
        | div_type | result     |
        | bigint   | 2147483648 |
      Then query schema
        """
        root
         |-- div_type: string (nullable = false)
         |-- result: long (nullable = false)
        """

    Scenario Outline: Zero DIV retains BIGINT for <case> outside ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 DIV <divisor> AS result, typeof(1 DIV <divisor>) AS result_type
        FROM range(1)
        """
      Then query result
        | result | result_type |
        | NULL   | bigint      |

      Examples:
        | case           | divisor              |
        | integer zero   | 0                    |
        | decimal zero   | 0.0BD                |
        | cast zero      | CAST(0 AS SMALLINT)   |
        | dynamic zero   | id                   |
