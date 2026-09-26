Feature: Conditional coercion preserves nested metadata

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
