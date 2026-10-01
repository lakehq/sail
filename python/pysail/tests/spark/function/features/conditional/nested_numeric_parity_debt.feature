Feature: Deferred nested numeric conditional parity

  # These coercion gaps also occur at the PR merge-base. They are not regressions.
  @sail-bug
  Scenario Outline: ANSI numeric widening retains integral precision inside <container>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT id, if_value<leaf> AS if_result, case_value<leaf> AS case_result,
             nvl2_value<leaf> AS nvl2_result, typeof(if_value) AS result_type
      FROM (
        SELECT id,
               IF(id = 0, <integral>, <floating>) AS if_value,
               CASE WHEN id = 0 THEN <integral> ELSE <floating> END AS case_value,
               NVL2(NULLIF(id, 1), <integral>, <floating>) AS nvl2_value
        FROM range(2)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | if_result  | case_result | nvl2_result | result_type |
      | 0  | 16777217.0 | 16777217.0  | 16777217.0  | <type>      |
      | 1  | 2.5        | 2.5         | 2.5         | <type>      |

    Examples:
      | container | integral                    | floating                             | leaf  | type               |
      | ARRAY     | array(16777217)             | array(CAST(2.5 AS FLOAT))             | [0]   | array<double>      |
      | STRUCT    | named_struct('x', 16777217) | named_struct('x', CAST(2.5 AS FLOAT)) | .x    | struct<x:double>   |
      | MAP       | map('k', 16777217)          | map('k', CAST(2.5 AS FLOAT))          | ['k'] | map<string,double> |

  @sail-bug
  Scenario Outline: Nested FLOAT and DECIMAL branches use DOUBLE with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, if_value[0] AS if_result, case_value[0] AS case_result,
             nvl2_value[0] AS nvl2_result, typeof(if_value) AS result_type
      FROM (
        SELECT id,
               IF(id = 0, array(CAST(1.25 AS FLOAT)), array(CAST(2.5 AS DECIMAL(12,2)))) AS if_value,
               CASE WHEN id = 0 THEN array(CAST(1.25 AS FLOAT))
                    ELSE array(CAST(2.5 AS DECIMAL(12,2))) END AS case_value,
               NVL2(NULLIF(id, 1), array(CAST(1.25 AS FLOAT)), array(CAST(2.5 AS DECIMAL(12,2)))) AS nvl2_value
        FROM range(2)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | if_result | case_result | nvl2_result | result_type   |
      | 0  | 1.25      | 1.25        | 1.25        | array<double> |
      | 1  | 2.5       | 2.5         | 2.5         | array<double> |

    Examples:
      | ansi  |
      | true  |
      | false |

  @spark-4.0
  @sail-bug
  Scenario Outline: Nested DECIMAL branches retain integral digits with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = false
    When query
      """
      SELECT id, if_value[0] AS if_result, case_value[0] AS case_result,
             nvl2_value[0] AS nvl2_result, typeof(if_value) AS result_type
      FROM (
        SELECT id,
               IF(id = 0, array(CAST(1.5 AS DECIMAL(38,20))), array(CAST(2 AS DECIMAL(38,0)))) AS if_value,
               CASE WHEN id = 0 THEN array(CAST(1.5 AS DECIMAL(38,20)))
                    ELSE array(CAST(2 AS DECIMAL(38,0))) END AS case_value,
               NVL2(NULLIF(id, 1), array(CAST(1.5 AS DECIMAL(38,20))), array(CAST(2 AS DECIMAL(38,0)))) AS nvl2_value
        FROM range(2)
      ) ORDER BY id
      """
    Then query result collected ordered
      | id | if_result | case_result | nvl2_result | result_type          |
      | 0  | 2         | 2           | 2           | array<decimal(38,0)> |
      | 1  | 2         | 2           | 2           | array<decimal(38,0)> |

    Examples:
      | ansi  |
      | true  |
      | false |

  @sail-bug
  Scenario Outline: Exhaustive numeric CASE declares non-nullable output with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT CASE WHEN id = 0 THEN 1L ELSE 2L END AS v FROM range(2)
      """
    Then query result
      | v |
      | 1 |
      | 2 |
    And query schema
      """
      root
       |-- v: long (nullable = false)
      """

    Examples:
      | ansi  |
      | true  |
      | false |
