Feature: Deferred numeric conditional coercion parity

  Scenario: Dynamic STRING branches do not change numeric sibling typing by projection order
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT typeof(if(id = 0, 1, 2L)) AS before_type,
             if(id = 0, 1, s) AS dynamic_value,
             typeof(if(id = 0, 1, 2L)) AS after_type
      FROM VALUES (0, '7'), (1, '8') AS t(id, s)
      ORDER BY id
      """
    Then query result ordered
      | before_type | dynamic_value | after_type |
      | bigint      | 1             | bigint     |
      | bigint      | 8             | bigint     |

  @sail-bug
  Scenario Outline: Conditional widening rounds high-scale DECIMAL to DOUBLE with <floating_type> and ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT if(id = 0, CAST(0.12 AS DECIMAL(38,37)), CAST(0.25 AS <floating_type>)) AS if_result,
             CASE WHEN id = 0 THEN CAST(0.12 AS DECIMAL(38,37))
                  ELSE CAST(0.25 AS <floating_type>) END AS case_result,
             nvl2(nullif(id, 1), CAST(0.12 AS DECIMAL(38,37)), CAST(0.25 AS <floating_type>)) AS nvl2_result
      FROM range(2) ORDER BY id
      """
    Then query result collected ordered
      | if_result | case_result | nvl2_result |
      | 0.12      | 0.12        | 0.12        |
      | 0.25      | 0.25        | 0.25        |

    Examples:
      | floating_type | ansi  |
      | FLOAT         | false |
      | DOUBLE        | false |
      | FLOAT         | true  |
      | DOUBLE        | true  |
