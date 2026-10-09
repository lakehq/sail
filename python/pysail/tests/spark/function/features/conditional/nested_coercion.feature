Feature: Conditional coercion of nested types

  Scenario Outline: Conditional numeric widening preserves an interval qualifier inside <kind>
    When query
      """
      SELECT id,
             CAST(if_result<access>.v AS INT) AS if_years,
             CAST(case_result<access>.v AS INT) AS case_years,
             CAST(nvl2_result<access>.v AS INT) AS nvl2_years,
             typeof(if_result<access>.n) AS if_number_type,
             typeof(case_result<access>.n) AS case_number_type,
             typeof(nvl2_result<access>.n) AS nvl2_number_type
      FROM (
        SELECT id,
               if(id = 0, <first>, <second>) AS if_result,
               CASE WHEN id = 0 THEN <first> ELSE <second> END AS case_result,
               nvl2(nullif(id, 1), <first>, <second>) AS nvl2_result
        FROM range(2)
      ) AS q
      ORDER BY id
      """
    Then query result ordered
      | id | if_years | case_years | nvl2_years | if_number_type | case_number_type | nvl2_number_type |
      | 0  | 1        | 1          | 1          | bigint         | bigint           | bigint           |
      | 1  | NULL     | NULL       | NULL       | bigint         | bigint           | bigint           |

    Examples:
      | kind                | first                                               | second                                                         | access   |
      | a struct            | struct(INTERVAL '1' YEAR AS v, 1 AS n)              | struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n)              |          |
      | an array of structs | array(struct(INTERVAL '1' YEAR AS v, 1 AS n))       | array(struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n))       | [0]      |
      | a map of structs    | map('item', struct(INTERVAL '1' YEAR AS v, 1 AS n)) | map('item', struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n)) | ['item'] |

  Scenario: Conditional numeric widening preserves a day-time interval qualifier
    When query
      """
      SELECT id,
             CAST(if_result.v AS STRING) AS if_days,
             CAST(case_result.v AS STRING) AS case_days,
             CAST(nvl2_result.v AS STRING) AS nvl2_days
      FROM (
        SELECT id,
               if(id = 0, first, second) AS if_result,
               CASE WHEN id = 0 THEN first ELSE second END AS case_result,
               nvl2(nullif(id, 1), first, second) AS nvl2_result
        FROM (
          SELECT id,
                 struct(INTERVAL '1' DAY AS v, 1 AS n) AS first,
                 struct(CAST(NULL AS INTERVAL DAY) AS v, 2L AS n) AS second
          FROM range(2)
        ) AS branches
      ) AS q
      ORDER BY id
      """
    Then query result ordered
      | id | if_days          | case_days        | nvl2_days        |
      | 0  | INTERVAL '1' DAY | INTERVAL '1' DAY | INTERVAL '1' DAY |
      | 1  | NULL             | NULL             | NULL             |

  @sail-bug
  Scenario Outline: ANSI <kind> rejects incompatible numeric and STRING map keys
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT <expression> AS result FROM range(2) ORDER BY id
      """
    Then query error DATATYPE_MISMATCH

    Examples:
      | kind | expression                                           |
      | IF   | if(id = 0, map(1, 1), map('2', 2))                   |
      | CASE | CASE WHEN id = 0 THEN map(1, 1) ELSE map('2', 2) END |

  @sail-bug
  Scenario Outline: Conditional numeric widening merges nested year-month qualifiers in <kind>
    When query
      """
      SELECT id,
             CAST(if_result<access>.v AS INT) AS if_months,
             CAST(case_result<access>.v AS INT) AS case_months,
             CAST(nvl2_result<access>.v AS INT) AS nvl2_months
      FROM (
        SELECT id,
               if(id = 0, <first>, <second>) AS if_result,
               CASE WHEN id = 0 THEN <first> ELSE <second> END AS case_result,
               nvl2(nullif(id, 1), <first>, <second>) AS nvl2_result
        FROM range(2)
      ) AS q ORDER BY id
      """
    Then query result ordered
      | id | if_months | case_months | nvl2_months |
      | 0  | 12        | 12          | 12          |
      | 1  | 2         | 2           | 2           |

    Examples:
      | kind                | first                                               | second                                                 | access   |
      | a struct            | struct(INTERVAL '1' YEAR AS v, 1 AS n)              | struct(INTERVAL '2' MONTH AS v, 2L AS n)               |          |
      | an array of structs | array(struct(INTERVAL '1' YEAR AS v, 1 AS n))       | array(struct(INTERVAL '2' MONTH AS v, 2L AS n))        | [0]      |
      | a map of structs    | map('item', struct(INTERVAL '1' YEAR AS v, 1 AS n)) | map('item', struct(INTERVAL '2' MONTH AS v, 2L AS n)) | ['item'] |

  @sail-bug
  Scenario Outline: Conditional numeric widening merges nested day-time qualifiers in <kind>
    When query
      """
      SELECT id,
             CAST(if_result<access>.v AS STRING) AS if_interval,
             CAST(case_result<access>.v AS STRING) AS case_interval,
             CAST(nvl2_result<access>.v AS STRING) AS nvl2_interval
      FROM (
        SELECT id,
               if(id = 0, <first>, <second>) AS if_result,
               CASE WHEN id = 0 THEN <first> ELSE <second> END AS case_result,
               nvl2(nullif(id, 1), <first>, <second>) AS nvl2_result
        FROM range(2)
      ) AS q ORDER BY id
      """
    Then query result ordered
      | id | if_interval                   | case_interval                 | nvl2_interval                 |
      | 0  | INTERVAL '1 00' DAY TO HOUR | INTERVAL '1 00' DAY TO HOUR | INTERVAL '1 00' DAY TO HOUR |
      | 1  | INTERVAL '0 02' DAY TO HOUR | INTERVAL '0 02' DAY TO HOUR | INTERVAL '0 02' DAY TO HOUR |

    Examples:
      | kind                | first                                              | second                                                | access   |
      | a struct            | struct(INTERVAL '1' DAY AS v, 1 AS n)              | struct(INTERVAL '2' HOUR AS v, 2L AS n)               |          |
      | an array of structs | array(struct(INTERVAL '1' DAY AS v, 1 AS n))       | array(struct(INTERVAL '2' HOUR AS v, 2L AS n))        | [0]      |
      | a map of structs    | map('item', struct(INTERVAL '1' DAY AS v, 1 AS n)) | map('item', struct(INTERVAL '2' HOUR AS v, 2L AS n)) | ['item'] |

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
