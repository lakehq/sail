Feature: Bitwise complement preserves integer width

  Scenario Outline: Bitwise complement handles minimum integers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT ~(-2147483648) AS int_result, typeof(~(-2147483648)) AS int_type,
             ~CAST(-128 AS TINYINT) AS tiny_result, typeof(~CAST(-128 AS TINYINT)) AS tiny_type,
             ~CAST(-32768 AS SMALLINT) AS small_result, typeof(~CAST(-32768 AS SMALLINT)) AS small_type,
             ~CAST('-9223372036854775808' AS BIGINT) AS long_result,
             typeof(~CAST('-9223372036854775808' AS BIGINT)) AS long_type
      """
    Then query result
      | int_result | int_type | tiny_result | tiny_type | small_result | small_type | long_result         | long_type |
      | 2147483647 | int      | 127         | tinyint   | 32767        | smallint   | 9223372036854775807 | bigint    |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: Bitwise complement aliases preserve integer columns and NULL with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, ~value AS operator_result, bitwise_not(value) AS function_result,
             typeof(~value) AS result_type
      FROM VALUES (0, -2147483648), (1, -2147483647), (2, 0),
                  (3, 2147483647), (4, CAST(NULL AS INT)) AS t(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | operator_result | function_result | result_type |
      | 0  | 2147483647      | 2147483647      | int         |
      | 1  | 2147483646      | 2147483646      | int         |
      | 2  | -1              | -1              | int         |
      | 3  | -2147483648     | -2147483648     | int         |
      | 4  | NULL            | NULL            | int         |

    Examples:
      | ansi  |
      | true  |
      | false |
