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
