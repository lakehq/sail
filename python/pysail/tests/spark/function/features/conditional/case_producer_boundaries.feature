Feature: Conditional observations preserve producer types across relational boundaries

  Scenario Outline: UNION observations inspect every producing branch with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT typeof(if(false, 0, c)) AS narrow_type,
        typeof(if(false, 0, wide)) AS wide_type
      FROM (
        SELECT regexp_instr('aba', 'a') AS c, regexp_instr('aba', 'a') AS wide
        UNION ALL
        SELECT regexp_instr('aba', 'a'), CAST(regexp_instr('aba', 'a') AS BIGINT)
      ) producer
      """
    Then query result collected
      | narrow_type | wide_type |
      | int         | bigint    |
      | int         | bigint    |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: VALUES observations inspect every producing row with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT typeof(if(false, 0, c)) AS narrow_type,
        typeof(if(false, 0, wide)) AS wide_type
      FROM VALUES
        (regexp_instr('aba', 'a'), regexp_instr('aba', 'a')),
        (regexp_instr('aba', 'a'), CAST(regexp_instr('aba', 'a') AS BIGINT))
      AS producer(c, wide)
      """
    Then query result collected
      | narrow_type | wide_type |
      | int         | bigint    |
      | int         | bigint    |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Scalar subquery observations preserve explicit target types with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT typeof(if(false, 0, (SELECT regexp_instr('aba', 'a')))) AS narrow_type,
        typeof(if(false, 0, (SELECT CAST(regexp_instr('aba', 'a') AS BIGINT)))) AS wide_type
      """
    Then query result collected
      | narrow_type | wide_type |
      | int         | bigint    |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Lateral right aliases retain correlated producer types with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT l.id, typeof(if(false, 0, r.c)) AS narrow_type,
        typeof(if(false, 0, r.wide)) AS wide_type, r.observed AS correlated_type
      FROM (
        SELECT id, regexp_instr(s, 'a') AS c,
          CAST(regexp_instr(s, 'a') AS BIGINT) AS wide
        FROM VALUES (0, 'aba'), (1, 'ba') t(id, s)
      ) l JOIN LATERAL (
        SELECT l.c AS c, l.wide AS wide, typeof(if(false, 0, l.c)) AS observed
      ) r ON true
      """
    Then query result collected
      | id | narrow_type | wide_type | correlated_type |
      | 0  | int         | bigint    | int             |
      | 1  | int         | bigint    | int             |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Regex shift observations preserve the original INT conditional with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        typeof(if(false, 0, shiftrightunsigned(regexp_count('aba', 'a'), 1))) AS direct_type,
        typeof(if(false, 0, shiftrightunsigned(c, 1))) AS input_type,
        typeof(if(false, 0, shifted)) AS projected_type,
        typeof(if(false, 0, shifted + 0)) AS arithmetic_type,
        typeof(if(false, 0, shifted + 0L)) AS bigint_sibling_type,
        typeof(CASE WHEN false THEN 0 WHEN true THEN shifted ELSE 1L END) AS bigint_branch_type
      FROM (
        SELECT regexp_count('aba', 'a') AS c,
          shiftrightunsigned(regexp_count('aba', 'a'), 1) AS shifted
      ) producer
      """
    Then query result collected
      | direct_type | input_type | projected_type | arithmetic_type | bigint_sibling_type | bigint_branch_type |
      | int         | int        | int            | int             | bigint              | bigint             |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: A projected BIGINT-first conditional keeps its original observation boundary with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT if(false, c,
        CAST(shiftrightunsigned(regexp_count(s, 'a'), 1) AS BIGINT)) AS value
      FROM (
        SELECT s, if(false, regexp_count(s, 'a'), 0) AS c
        FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ) producer
      """
    Then query result collected
      | value |
      | 1     |
      | NULL  |
    Then query schema
      """
      root
       |-- value: long (nullable = true)
      """

    Examples:
      | ansi  |
      | false |
      | true  |

  @sail-bug
  Scenario Outline: Deferred explicit BIGINT shifted casts retain their declared conditional type with ANSI <ansi>
    # The observation fallback preserves the baseline INT conditional while
    # generated shift casts and explicit casts remain opaque. Restoring precise
    # cast origins must precede widening this class, including arithmetic inside it.
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        typeof(if(false, 0,
          CAST(shiftrightunsigned(regexp_count('aba', 'a'), 1) AS BIGINT))) AS direct_type,
        typeof(if(false, 0, wide)) AS projected_type,
        typeof(if(false, 0, CAST(
          shiftrightunsigned(regexp_count('aba', 'a'), 1) + 3000000000L AS BIGINT))) AS arithmetic_type
      FROM (SELECT CAST(shiftrightunsigned(regexp_count('aba', 'a'), 1) AS BIGINT) AS wide) producer
      """
    Then query result collected
      | direct_type | projected_type | arithmetic_type |
      | bigint      | bigint         | bigint          |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario: Repeated conditional branches reuse their scoped producer types
    When query
      """
      WITH s0 AS (SELECT 1L AS c),
        s1 AS (SELECT if(false, c, c) AS c FROM s0),
        s2 AS (SELECT if(false, c, c) AS c FROM s1),
        s3 AS (SELECT if(false, c, c) AS c FROM s2),
        s4 AS (SELECT if(false, c, c) AS c FROM s3),
        s5 AS (SELECT if(false, c, c) AS c FROM s4),
        s6 AS (SELECT if(false, c, c) AS c FROM s5),
        s7 AS (SELECT if(false, c, c) AS c FROM s6),
        s8 AS (SELECT if(false, c, c) AS c FROM s7),
        s9 AS (SELECT if(false, c, c) AS c FROM s8),
        s10 AS (SELECT if(false, c, c) AS c FROM s9),
        s11 AS (SELECT if(false, c, c) AS c FROM s10),
        s12 AS (SELECT if(false, c, c) AS c FROM s11)
      SELECT typeof(if(false, c, c)) AS value_type FROM s12
      """
    Then query result collected
      | value_type |
      | bigint     |

  Scenario Outline: Lateral CASE result schemas retain correlated producers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT r.c
      FROM (
        SELECT regexp_instr(s, 'a') AS x
        FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ) l JOIN LATERAL (SELECT if(false, 0, l.x) AS c) r ON true
      ORDER BY c NULLS FIRST
      """
    Then query result collected
      | c    |
      | NULL |
      | 1    |
    Then query schema
      """
      root
       |-- c: integer (nullable = true)
      """
    When query
      """
      SELECT r.c
      FROM (
        SELECT CAST(regexp_instr(s, 'a') AS BIGINT) AS x
        FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ) l JOIN LATERAL (SELECT if(false, 0, l.x) AS c) r ON true
      ORDER BY c NULLS FIRST
      """
    Then query result collected
      | c    |
      | NULL |
      | 1    |
    Then query schema
      """
      root
       |-- c: long (nullable = true)
      """

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Direct transform CASE observations retain lambda element producers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT transform(array(regexp_instr(s, 'a')), x -> if(false, 0, x)) AS result,
        typeof(transform(array(regexp_instr(s, 'a')), x -> if(false, 0, x))) AS observed_type
      FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ORDER BY s NULLS FIRST
      """
    Then query result collected
      | result | observed_type |
      | [None] | array<int>    |
      | [1]    | array<int>    |
    Then query schema
      """
      root
       |-- result: array (nullable = false)
       |    |-- element: integer (containsNull = true)
       |-- observed_type: string (nullable = false)
      """
    When query
      """
      SELECT transform(CAST(array(regexp_instr(s, 'a')) AS ARRAY<BIGINT>),
        x -> if(false, 0, x)) AS result,
        typeof(transform(CAST(array(regexp_instr(s, 'a')) AS ARRAY<BIGINT>),
          x -> if(false, 0, x))) AS observed_type
      FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ORDER BY s NULLS FIRST
      """
    Then query result collected
      | result | observed_type |
      | [None] | array<bigint> |
      | [1]    | array<bigint> |
    Then query schema
      """
      root
       |-- result: array (nullable = false)
       |    |-- element: long (containsNull = true)
       |-- observed_type: string (nullable = false)
      """

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Projected transform CASE observations retain lambda element producers with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT transform(a, x -> if(false, 0, x)) AS result,
        typeof(transform(a, x -> if(false, 0, x))) AS observed_type
      FROM (
        SELECT s, array(regexp_instr(s, 'a')) AS a
        FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ) p ORDER BY s NULLS FIRST
      """
    Then query result collected
      | result | observed_type |
      | [None] | array<int>    |
      | [1]    | array<int>    |
    Then query schema
      """
      root
       |-- result: array (nullable = false)
       |    |-- element: integer (containsNull = true)
       |-- observed_type: string (nullable = false)
      """
    When query
      """
      SELECT transform(a, x -> if(false, 0, x)) AS result,
        typeof(transform(a, x -> if(false, 0, x))) AS observed_type
      FROM (
        SELECT s, CAST(array(regexp_instr(s, 'a')) AS ARRAY<BIGINT>) AS a
        FROM VALUES ('aba'), (CAST(NULL AS STRING)) t(s)
      ) p ORDER BY s NULLS FIRST
      """
    Then query result collected
      | result | observed_type |
      | [None] | array<bigint> |
      | [1]    | array<bigint> |
    Then query schema
      """
      root
       |-- result: array (nullable = false)
       |    |-- element: long (containsNull = true)
       |-- observed_type: string (nullable = false)
      """

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Index-only transform observations keep the index type with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        transform(array(9L, 8L), (x, i) -> if(false, 0, i)) AS value,
        typeof(transform(array(9L, 8L), (x, i) -> if(false, 0, i))) AS observed_type
      """
    Then query result collected
      | value  | observed_type |
      | [0, 1] | array<int>    |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Element-only aggregate merge observations keep their parameter order with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        aggregate(array(3L, 5L), 0L, (acc, x) -> if(false, 0L, x),
          acc -> if(false, 0L, acc)) AS value,
        typeof(aggregate(array(3L, 5L), 0L, (acc, x) -> if(false, 0L, x),
          acc -> if(false, 0L, acc))) AS observed_type
      """
    Then query result collected
      | value | observed_type |
      | 5     | bigint        |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Signed intermediate lambda elements retain their observations with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        transform(array(CAST(2 AS TINYINT) + CAST(1 AS SMALLINT)),
          x -> if(false, 0, x + regexp_instr('aba', 'a'))) AS value,
        typeof(transform(array(CAST(2 AS TINYINT) + CAST(1 AS SMALLINT)),
          x -> if(false, 0, x + regexp_instr('aba', 'a')))) AS observed_type
      """
    Then query result collected
      | value | observed_type |
      | [4]   | array<int>    |

    Examples:
      | ansi  |
      | false |
      | true  |
