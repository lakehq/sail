Feature: Conditional branch type coercion

  Rule: Type hints do not alter conditionals that already expose their common type

    Scenario Outline: Conditionals whose first branch already exposes the common type preserve rounding with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          CAST(if(p, round(if(q, 0.5D,
            CAST('99999999999999999999999.5' AS DECIMAL(38,15))), 0), 1)
            AS DOUBLE) AS if_value,
          CAST(CASE WHEN p THEN round(if(q, 0.5D,
            CAST('99999999999999999999999.5' AS DECIMAL(38,15))), 0)
            ELSE 1 END AS DOUBLE) AS case_value,
          CAST(CASE WHEN p IS NULL THEN NULL
            WHEN p THEN round(if(q, 0.5D,
              CAST('99999999999999999999999.5' AS DECIMAL(38,15))), 0)
            ELSE 1 END AS DOUBLE) AS null_first_value
        FROM VALUES (0, true, false), (1, false, true), (2, NULL, false) AS t(id, p, q)
        """
      Then query result collected
        | id | if_value | case_value | null_first_value |
        | 0  | 1e+23    | 1e+23      | 1e+23            |
        | 1  | 1.0      | 1.0        | 1.0              |
        | 2  | 1.0      | 1.0        | NULL             |

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: Type hints preserve the analysis of the original branches

    Scenario Outline: Numeric type hints preserve late decimal rounding with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH s AS (
          SELECT id, p, q,
            round(if(q, 0.5D,
              CAST('99999999999999999999999.5' AS DECIMAL(38,15))), 0) AS rounded
          FROM VALUES (0, false, false), (1, true, true), (2, NULL, false) AS t(id, p, q)
        )
        SELECT id,
          CAST(if(p, 1, round(if(q, 0.5D,
            CAST('99999999999999999999999.5' AS DECIMAL(38,15))), 0))
            AS DOUBLE) AS inline_if,
          CAST(CASE WHEN p THEN 1 ELSE round(CASE WHEN q THEN 0.5D
            ELSE CAST('99999999999999999999999.5' AS DECIMAL(38,15)) END, 0)
            END AS DOUBLE) AS inline_case,
          CAST(if(p, 1, rounded) AS DOUBLE) AS projected_if,
          CAST(CASE WHEN p THEN 1 ELSE rounded END AS DOUBLE) AS projected_case
        FROM s
        """
      Then query result collected
        | id | inline_if | inline_case | projected_if | projected_case |
        | 0  | 1e+23     | 1e+23       | 1e+23        | 1e+23          |
        | 1  | 1.0       | 1.0         | 1.0          | 1.0            |
        | 2  | 1e+23     | 1e+23       | 1e+23        | 1e+23          |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: Conditional type hints preserve literal evaluation before analysis with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT 'if' AS kind, count(*) AS n FROM range(if(true, 3, 4L))
        UNION ALL
        SELECT 'case', count(*)
        FROM range(CASE WHEN true THEN 3 ELSE CAST('unselected' AS BIGINT) END)
        UNION ALL
        SELECT 'nested', count(*) FROM range(1 + if(true, 2, 4L))
        """
      Then query result collected
        | kind   | n |
        | if     | 3 |
        | case   | 3 |
        | nested | 3 |

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: Resolve all branch types before exposing the conditional result type

    Scenario Outline: CASE and IF typeof include both <left_type> and <right_type> branches
      When query
        """
        SELECT
          typeof(CASE WHEN false THEN CAST(1 AS <left_type>) ELSE CAST(1 AS <right_type>) END) AS case_type,
          typeof(CASE WHEN true THEN CAST(1 AS <right_type>) ELSE CAST(1 AS <left_type>) END) AS reversed_type,
          typeof(if(false, CAST(1 AS <left_type>), CAST(1 AS <right_type>))) AS if_type
        """
      Then query result collected
        | case_type     | reversed_type | if_type       |
        | <result_type> | <result_type> | <result_type> |

      Examples:
        | left_type    | right_type    | result_type   |
        | INT          | BIGINT        | bigint        |
        | SMALLINT     | BIGINT        | bigint        |
        | INT          | DOUBLE        | double        |
        | DECIMAL(8,2) | DECIMAL(10,4) | decimal(10,4) |

    @sail-bug
    Scenario Outline: Deferred integral-first decimal branches expose their common type in either order
      When query
        """
        SELECT
          typeof(CASE WHEN false THEN CAST(1 AS <left_type>) ELSE CAST(1 AS <right_type>) END) AS case_type,
          typeof(CASE WHEN true THEN CAST(1 AS <right_type>) ELSE CAST(1 AS <left_type>) END) AS reversed_type,
          typeof(if(false, CAST(1 AS <left_type>), CAST(1 AS <right_type>))) AS if_type
        """
      Then query result collected
        | case_type     | reversed_type | if_type       |
        | <result_type> | <result_type> | <result_type> |

      Examples:
        | left_type    | right_type    | result_type   |
        | INT          | DECIMAL(20,0) | decimal(20,0) |
        | DECIMAL(8,2) | BIGINT        | decimal(22,2) |

    @sail-bug
    Scenario Outline: Deferred decimal and <fractional_type> conditional branches resolve to DOUBLE
      When query
        """
        SELECT
          typeof(CASE WHEN false THEN CAST(1 AS DECIMAL(8,2))
                      ELSE CAST(1 AS <fractional_type>) END) AS case_type,
          typeof(CASE WHEN true THEN CAST(1 AS <fractional_type>)
                      ELSE CAST(1 AS DECIMAL(8,2)) END) AS reversed_type,
          typeof(if(false, CAST(1 AS DECIMAL(8,2)), CAST(1 AS <fractional_type>))) AS if_type
        """
      Then query result collected
        | case_type | reversed_type | if_type |
        | double    | double        | double  |

      Examples:
        | fractional_type |
        | FLOAT           |
        | DOUBLE          |

    Scenario Outline: Dynamic <wide_type> branches agree with their declared schema and preserve collected values
      When query
        """
        SELECT
          CASE WHEN p THEN n ELSE CAST(w AS <wide_type>) END AS case_result,
          CASE WHEN NOT p THEN CAST(w AS <wide_type>) ELSE n END AS reversed_case,
          if(p, n, CAST(w AS <wide_type>)) AS if_result,
          if(NOT p, CAST(w AS <wide_type>), n) AS reversed_if
        FROM VALUES (true, 1, <wide_value>), (false, 1, <wide_value>), (NULL, NULL, NULL) AS t(p, n, w)
        """
      Then query schema
        """
        root
         |-- case_result: <schema_type> (nullable = true)
         |-- reversed_case: <schema_type> (nullable = true)
         |-- if_result: <schema_type> (nullable = true)
         |-- reversed_if: <schema_type> (nullable = true)
        """
      And query result collected
        | case_result  | reversed_case | if_result    | reversed_if  |
        | <one>        | <one>         | <one>        | <one>        |
        | <wide_value> | <wide_value>  | <wide_value> | <wide_value> |
        | NULL         | NULL          | NULL         | NULL         |

      Examples:
        | wide_type     | wide_value | schema_type   | one  |
        | BIGINT        | 3000000000 | long          | 1    |
        | DOUBLE        | 1.5        | double        | 1.0  |

    @sail-bug
    Scenario: Deferred dynamic integral-first decimal branches agree with their declared schema
      When query
        """
        SELECT
          CASE WHEN p THEN n ELSE CAST(w AS DECIMAL(10,2)) END AS case_result,
          CASE WHEN NOT p THEN CAST(w AS DECIMAL(10,2)) ELSE n END AS reversed_case,
          if(p, n, CAST(w AS DECIMAL(10,2))) AS if_result,
          if(NOT p, CAST(w AS DECIMAL(10,2)), n) AS reversed_if
        FROM VALUES (true, 1, 1.75), (false, 1, 1.75), (NULL, NULL, NULL) AS t(p, n, w)
        """
      Then query schema
        """
        root
         |-- case_result: decimal(12,2) (nullable = true)
         |-- reversed_case: decimal(12,2) (nullable = true)
         |-- if_result: decimal(12,2) (nullable = true)
         |-- reversed_if: decimal(12,2) (nullable = true)
        """
      And query result collected
        | case_result | reversed_case | if_result | reversed_if |
        | 1.00        | 1.00          | 1.00      | 1.00        |
        | 1.75        | 1.75          | 1.75      | 1.75        |
        | NULL        | NULL          | NULL      | NULL        |

    Scenario: NULL branches and omitted ELSE preserve the common numeric type
      When query
        """
        SELECT
          CASE WHEN id = 0 THEN NULL WHEN id = 1 THEN 1 ELSE 3000000000L END AS null_first,
          CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 3000000000L END AS no_else,
          CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 3000000000L ELSE NULL END AS null_else
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query schema
        """
        root
         |-- null_first: long (nullable = true)
         |-- no_else: long (nullable = true)
         |-- null_else: long (nullable = true)
        """
      And query result collected
        | null_first | no_else    | null_else  |
        | NULL       | NULL       | NULL       |
        | 1          | 1          | 1          |
        | 3000000000 | 3000000000 | 3000000000 |

    Scenario: Multibranch simple nested and projected conditionals expose their final numeric type
      When query
        """
        WITH s AS (
          SELECT id, CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS DOUBLE) END AS projected
          FROM VALUES (0), (1), (2) AS t(id)
        )
        SELECT
          CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(1.5 AS DOUBLE) END AS multibranch,
          CASE id WHEN 0 THEN 1 WHEN 1 THEN CAST(1.5 AS DOUBLE) END AS simple_case,
          CASE WHEN id < 2 THEN CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS DOUBLE) END END AS nested_case,
          if(id < 2, if(id = 0, 1, CAST(1.5 AS DOUBLE)), NULL) AS nested_if,
          CASE WHEN id < 2 THEN projected END AS projected_case
        FROM s
        """
      Then query schema
        """
        root
         |-- multibranch: double (nullable = true)
         |-- simple_case: double (nullable = true)
         |-- nested_case: double (nullable = true)
         |-- nested_if: double (nullable = true)
         |-- projected_case: double (nullable = true)
        """
      And query result collected
        | multibranch | simple_case | nested_case | nested_if | projected_case |
        | 1.0         | 1.0         | 1.0         | 1.0       | 1.0            |
        | 1.5         | 1.5         | 1.5         | 1.5       | 1.5            |
        | NULL        | NULL        | NULL        | NULL      | NULL           |

    @spark-4
    @sail-bug
    Scenario Outline: Deferred decimal scale clipping rounds conditional branch values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          CASE WHEN p THEN CAST(n AS DECIMAL(38,0)) ELSE CAST(w AS DECIMAL(38,1)) END AS case_result,
          if(p, CAST(n AS DECIMAL(38,0)), CAST(w AS DECIMAL(38,1))) AS if_result
        FROM VALUES (true, 1, 0.5), (false, 1, 0.5), (false, 1, -0.5), (NULL, NULL, NULL) AS t(p, n, w)
        """
      Then query schema
        """
        root
         |-- case_result: decimal(38,0) (nullable = true)
         |-- if_result: decimal(38,0) (nullable = true)
        """
      And query result collected
        | case_result | if_result |
        | 1           | 1         |
        | 1           | 1         |
        | -1          | -1        |
        | NULL        | NULL      |

      Examples:
        | ansi  |
        | false |
        | true  |

    @spark-4
    @sail-bug
    Scenario: Deferred decimal scale clipping resolves the type when the fractional branch is first
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          typeof(CASE WHEN p THEN CAST(n AS DECIMAL(38,1))
                      ELSE CAST(m AS DECIMAL(38,0)) END) AS case_type,
          typeof(if(p, CAST(n AS DECIMAL(38,1)), CAST(m AS DECIMAL(38,0)))) AS if_type
        FROM VALUES (false, 0.5, 1) AS t(p, n, m)
        """
      Then query result collected
        | case_type     | if_type       |
        | decimal(38,0) | decimal(38,0) |

    Scenario: Integral and FLOAT branches preserve legacy precision
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          typeof(CASE WHEN p THEN 16777217 ELSE CAST(0.5 AS FLOAT) END) AS result_type,
          CASE WHEN p THEN 16777217 ELSE CAST(0.5 AS FLOAT) END AS case_result,
          if(p, 16777217, CAST(0.5 AS FLOAT)) AS if_result
        FROM VALUES (true), (false) AS t(p)
        """
      Then query result collected
        | result_type | case_result | if_result   |
        | float       | 16777216.0  | 16777216.0  |
        | float       | 0.5         | 0.5         |

    @sail-bug
    Scenario: Deferred ANSI integral and FLOAT branches preserve integer precision
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          typeof(CASE WHEN p THEN 16777217 ELSE CAST(0.5 AS FLOAT) END) AS result_type,
          CASE WHEN p THEN 16777217 ELSE CAST(0.5 AS FLOAT) END AS case_result,
          if(p, 16777217, CAST(0.5 AS FLOAT)) AS if_result
        FROM VALUES (true), (false) AS t(p)
        """
      Then query result collected
        | result_type | case_result | if_result   |
        | double      | 16777217.0  | 16777217.0  |
        | double      | 0.5         | 0.5         |

    @sail-bug
    Scenario: Deferred projected FLOAT and decimal conditionals retain DOUBLE string formatting
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CAST(CASE WHEN p THEN 1L ELSE c END AS STRING) AS case_case,
          CAST(CASE WHEN p THEN 1L ELSE i END AS STRING) AS case_if,
          CAST(if(p, 1L, c) AS STRING) AS if_case,
          CAST(if(p, 1L, i) AS STRING) AS if_if
        FROM (
          SELECT p,
            CASE WHEN q THEN CAST(0.5 AS FLOAT) ELSE CAST(0.5 AS DECIMAL(8,2)) END AS c,
            if(q, CAST(0.5 AS FLOAT), CAST(0.5 AS DECIMAL(8,2))) AS i
          FROM VALUES (true, false), (false, false) AS t(p, q)
        ) AS projected
        """
      Then query result collected
        | case_case | case_if | if_case | if_if |
        | 1.0       | 1.0     | 1.0     | 1.0   |
        | 0.5       | 0.5     | 0.5     | 0.5   |

  Rule: Numeric branch coercion preserves existing mixed branch behavior

    Scenario: Projected fractional branches preserve the capacity of a BIGINT sibling
      Given config spark.sql.ansi.enabled = false
      When query
        """
        WITH s AS (
          SELECT p,
            CASE WHEN q THEN CAST(0.5 AS FLOAT) ELSE CAST(0.5 AS DECIMAL(8,2)) END AS c
          FROM VALUES (true, false), (false, false) AS t(p, q)
        )
        SELECT
          CAST(CASE WHEN p THEN 3000000000L ELSE c END AS DOUBLE) AS case_result,
          CAST(if(p, 3000000000L, c) AS DOUBLE) AS if_result
        FROM s
        """
      Then query result collected
        | case_result  | if_result    |
        | 3000000000.0 | 3000000000.0 |
        | 0.5          | 0.5          |

    @sail-bug
    Scenario: ANSI BIGINT and string branches preserve numeric values in either branch order
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CASE WHEN p THEN n ELSE s END AS case_result,
          CASE WHEN NOT p THEN s ELSE n END AS reversed_case,
          if(p, n, s) AS if_result,
          if(NOT p, s, n) AS reversed_if
        FROM VALUES
          (0, true, 2L, '03'),
          (1, false, 2L, '03'),
          (2, false, 2L, '+03'),
          (3, false, 2L, '-03'),
          (4, false, 2L, ' 03 '),
          (5, NULL, NULL, NULL)
        AS t(id, p, n, s)
        ORDER BY id
        """
      Then query schema
        """
        root
         |-- case_result: long (nullable = true)
         |-- reversed_case: long (nullable = true)
         |-- if_result: long (nullable = true)
         |-- reversed_if: long (nullable = true)
        """
      And query result collected ordered
        | case_result | reversed_case | if_result | reversed_if |
        | 2           | 2             | 2         | 2           |
        | 3           | 3             | 3         | 3           |
        | 3           | 3             | 3         | 3           |
        | -3          | -3            | -3        | -3          |
        | 3           | 3             | 3         | 3           |
        | NULL        | NULL          | NULL      | NULL        |

    @sail-bug
    Scenario: ANSI DOUBLE and string branches preserve decimal and exponent values
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CASE WHEN p THEN CAST(n AS DOUBLE) ELSE s END AS case_result,
          CASE WHEN NOT p THEN s ELSE CAST(n AS DOUBLE) END AS reversed_case,
          if(p, CAST(n AS DOUBLE), s) AS if_result,
          if(NOT p, s, CAST(n AS DOUBLE)) AS reversed_if
        FROM VALUES
          (0, true, 2, '1.5'),
          (1, false, 2, '1.5'),
          (2, false, 2, '1e2'),
          (3, false, 2, '-1.5e1'),
          (4, NULL, NULL, NULL)
        AS t(id, p, n, s)
        ORDER BY id
        """
      Then query schema
        """
        root
         |-- case_result: double (nullable = true)
         |-- reversed_case: double (nullable = true)
         |-- if_result: double (nullable = true)
         |-- reversed_if: double (nullable = true)
        """
      And query result collected ordered
        | case_result | reversed_case | if_result | reversed_if |
        | 2.0         | 2.0           | 2.0       | 2.0         |
        | 1.5         | 1.5           | 1.5       | 1.5         |
        | 100.0       | 100.0         | 100.0     | 100.0       |
        | -15.0       | -15.0         | -15.0     | -15.0       |
        | NULL        | NULL          | NULL      | NULL        |

    Scenario: ANSI numeric-first scalar branches retain their numeric result type
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          typeof(CASE WHEN p THEN n ELSE s END) AS case_long_type,
          typeof(if(p, n, s)) AS if_long_type,
          typeof(CASE WHEN p THEN CAST(n AS DOUBLE) ELSE s END) AS case_double_type,
          typeof(if(p, CAST(n AS DOUBLE), s)) AS if_double_type
        FROM VALUES (false, 1L, '03') AS t(p, n, s)
        """
      Then query result collected
        | case_long_type | if_long_type | case_double_type | if_double_type |
        | bigint         | bigint       | double           | double         |

    Scenario: Nested decimal and string branches preserve legacy collected values before numeric rounding
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CASE WHEN p THEN CASE WHEN q THEN CAST(n AS DECIMAL(38,1)) ELSE s END
               ELSE CAST(m AS DECIMAL(38,0)) END AS case_case,
          CASE WHEN p THEN if(q, CAST(n AS DECIMAL(38,1)), s)
               ELSE CAST(m AS DECIMAL(38,0)) END AS case_if,
          if(p, CASE WHEN q THEN CAST(n AS DECIMAL(38,1)) ELSE s END,
             CAST(m AS DECIMAL(38,0))) AS if_case,
          if(p, if(q, CAST(n AS DECIMAL(38,1)), s), CAST(m AS DECIMAL(38,0))) AS if_if
        FROM VALUES
          (0, true, true, 1.5, 2L, 'unused'),
          (1, true, false, 1.5, 2L, 'a'),
          (2, true, false, 1.5, 2L, '03'),
          (3, false, false, 1.5, 2L, 'a'),
          (4, true, false, NULL, 2L, NULL)
        AS t(id, p, q, n, m, s)
        ORDER BY id
        """
      Then query result collected ordered
        | case_case | case_if | if_case | if_if |
        | 1.5       | 1.5     | 1.5     | 1.5   |
        | a         | a       | a       | a     |
        | 03        | 03      | 03      | 03    |
        | 2         | 2       | 2       | 2     |
        | NULL      | NULL    | NULL    | NULL  |

    Scenario: Projected decimal and string branches preserve legacy collected values before numeric rounding
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CASE WHEN p THEN c ELSE CAST(m AS DECIMAL(38,0)) END AS case_case,
          CASE WHEN p THEN i ELSE CAST(m AS DECIMAL(38,0)) END AS case_if,
          if(p, c, CAST(m AS DECIMAL(38,0))) AS if_case,
          if(p, i, CAST(m AS DECIMAL(38,0))) AS if_if
        FROM (
          SELECT id, p, m,
            CASE WHEN q THEN CAST(n AS DECIMAL(38,1)) ELSE s END AS c,
            if(q, CAST(n AS DECIMAL(38,1)), s) AS i
          FROM VALUES
            (0, true, true, 1.5, 2L, 'unused'),
            (1, true, false, 1.5, 2L, 'a'),
            (2, true, false, 1.5, 2L, '03'),
            (3, false, false, 1.5, 2L, 'a'),
            (4, true, false, NULL, 2L, NULL)
          AS t(id, p, q, n, m, s)
        ) AS projected
        ORDER BY id
        """
      Then query result collected ordered
        | case_case | case_if | if_case | if_if |
        | 1.5       | 1.5     | 1.5     | 1.5   |
        | a         | a       | a       | a     |
        | 03        | 03      | 03      | 03    |
        | 2         | 2       | 2       | 2     |
        | NULL      | NULL    | NULL    | NULL  |

    Scenario: Numeric siblings of projected mixed branches retain decimal value formatting
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CASE WHEN p THEN clipped ELSE CAST(0.55 AS DECIMAL(38,2)) END AS clipped_case,
          if(p, clipped, CAST(0.55 AS DECIMAL(38,2))) AS clipped_if,
          CASE WHEN p THEN expanded ELSE CAST(2 AS DECIMAL(1,0)) END AS expanded_case,
          if(p, expanded, CAST(2 AS DECIMAL(1,0))) AS expanded_if
        FROM (
          SELECT id, p,
            if(p, CAST(1 AS DECIMAL(38,0)), 'a') AS clipped,
            CASE WHEN q THEN CAST(1 AS DECIMAL(10,2)) ELSE 'a' END AS expanded
          FROM VALUES (0, true, false), (1, false, false) AS t(id, p, q)
        ) AS projected
        ORDER BY id
        """
      Then query result collected ordered
        | clipped_case | clipped_if | expanded_case | expanded_if |
        | 1            | 1          | a             | a           |
        | 0.55         | 0.55       | 2             | 2           |

    Scenario: ANSI numeric and string array branches retain their numeric result type
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          typeof(CASE WHEN p THEN array(n) ELSE array(s) END) AS case_type,
          typeof(if(p, array(n), array(s))) AS if_type
        FROM VALUES (false, 1L, '03') AS t(p, n, s)
        """
      Then query result collected
        | case_type     | if_type       |
        | array<bigint> | array<bigint> |

    @sail-bug
    Scenario: ANSI conditional arrays need recursive coercion to collect numeric string elements
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CASE WHEN p THEN array(n) ELSE array(s) END AS case_result,
          if(p, array(n), array(s)) AS if_result
        FROM VALUES (0, true, 1L, '03'), (1, false, 1L, '03') AS t(id, p, n, s)
        ORDER BY id
        """
      Then query result collected ordered
        | case_result | if_result |
        | [1]         | [1]       |
        | [3]         | [3]       |

  Rule: Consumers use the coerced conditional type

    Scenario Outline: Division preserves DOUBLE evaluation around deferred <decimal_type> branches with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH projected AS (
          SELECT p, q, d, w,
            CASE WHEN q THEN CAST(d AS <decimal_type>) ELSE CAST(w AS DOUBLE) END AS c,
            if(q, CAST(d AS <decimal_type>), CAST(w AS DOUBLE)) AS i
          FROM VALUES
            (true, true, '<decimal_value>', <floating_value>),
            (false, true, '<decimal_value>', <floating_value>),
            (false, false, '<decimal_value>', <floating_value>),
            (NULL, NULL, NULL, NULL)
          AS t(p, q, d, w)
        )
        SELECT
          (CASE WHEN p THEN 1 ELSE
            CASE WHEN q THEN CAST(d AS <decimal_type>) ELSE CAST(w AS DOUBLE) END
           END) / 3 AS inline_case,
          if(p, 1, if(q, CAST(d AS <decimal_type>), CAST(w AS DOUBLE))) / 3 AS inline_if,
          (CASE WHEN p THEN 1 ELSE c END) / 3 AS projected_case,
          if(p, 1, i) / 3 AS projected_if,
          (CASE WHEN p IS NULL THEN NULL WHEN p THEN 1 ELSE c END) / 3 AS null_first_case
        FROM projected
        """
      Then query schema
        """
        root
         |-- inline_case: double (nullable = true)
         |-- inline_if: double (nullable = true)
         |-- projected_case: double (nullable = true)
         |-- projected_if: double (nullable = true)
         |-- null_first_case: double (nullable = true)
        """
      And query result collected
        | inline_case          | inline_if            | projected_case       | projected_if         | null_first_case      |
        | 0.3333333333333333   | 0.3333333333333333   | 0.3333333333333333   | 0.3333333333333333   | 0.3333333333333333   |
        | <decimal_quotient>   | <decimal_quotient>   | <decimal_quotient>   | <decimal_quotient>   | <decimal_quotient>   |
        | <floating_quotient>  | <floating_quotient>  | <floating_quotient>  | <floating_quotient>  | <floating_quotient>  |
        | NULL                 | NULL                 | NULL                 | NULL                 | NULL                 |

      Examples:
        | ansi  | decimal_type   | decimal_value                         | floating_value | decimal_quotient     | floating_quotient   |
        | false | DECIMAL(8,2)   | 0.49                                  | 1.25           | 0.16333333333333333  | 0.4166666666666667   |
        | true  | DECIMAL(8,2)   | 0.49                                  | 1.25           | 0.16333333333333333  | 0.4166666666666667   |
        | false | DECIMAL(38,18) | 999999999999999999.499999999999999999 | 0.5            | 3.333333333333333e+17 | 0.16666666666666666  |
        | true  | DECIMAL(38,18) | 999999999999999999.499999999999999999 | 0.5            | 3.333333333333333e+17 | 0.16666666666666666  |

    Scenario Outline: Deferred fractional branches preserve reciprocal and rounded division with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH projected AS (
          SELECT p,
            CASE WHEN q THEN CAST(d AS DECIMAL(8,2)) ELSE CAST(w AS DOUBLE) END AS c,
            if(q, CAST(d AS DECIMAL(8,2)), CAST(w AS DOUBLE)) AS i
          FROM VALUES
            (true, true, '0.49', 1.25),
            (false, true, '0.49', 1.25),
            (false, false, '0.49', 1.25),
            (NULL, NULL, NULL, NULL)
          AS t(p, q, d, w)
        )
        SELECT
          3 / (CASE WHEN p THEN 1 ELSE c END) AS case_reciprocal,
          3 / if(p, 1, i) AS if_reciprocal,
          round(CASE WHEN p THEN 1 ELSE c END, 2) / 3 AS case_rounded,
          round(if(p, 1, i), 2) / 3 AS if_rounded
        FROM projected
        """
      Then query schema
        """
        root
         |-- case_reciprocal: double (nullable = true)
         |-- if_reciprocal: double (nullable = true)
         |-- case_rounded: double (nullable = true)
         |-- if_rounded: double (nullable = true)
        """
      And query result collected
        | case_reciprocal  | if_reciprocal    | case_rounded        | if_rounded          |
        | 3.0              | 3.0              | 0.3333333333333333   | 0.3333333333333333   |
        | 6.122448979591836 | 6.122448979591836 | 0.16333333333333333  | 0.16333333333333333  |
        | 2.4              | 2.4              | 0.4166666666666667   | 0.4166666666666667   |
        | NULL             | NULL             | NULL                | NULL                |

      Examples:
        | ansi  |
        | false |
        | true  |

    @sail-bug
    Scenario: Deferred integral-first decimal branches expose their element type to nested transforms
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id,
          transform(
            transform(a, x -> CASE WHEN x > 0 THEN CAST(x AS INT)
                                  ELSE CAST(x AS DECIMAL(12,2)) END),
            x -> named_struct('kind', typeof(x), 'v', x)
          ) AS result
        FROM VALUES
          (0, array(1, -2, CAST(NULL AS INT))),
          (1, CAST(array() AS ARRAY<INT>)),
          (2, CAST(NULL AS ARRAY<INT>))
        AS t(id, a)
        """
      Then query result collected
        | id | result                                                                                                                                                       |
        | 0  | [Row(kind='decimal(12,2)', v=Decimal('1.00')), Row(kind='decimal(12,2)', v=Decimal('-2.00')), Row(kind='decimal(12,2)', v=None)]                                  |
        | 1  | []                                                                                                                                                           |
        | 2  | NULL                                                                                                                                                         |

    @sail-bug
    Scenario: Deferred integral-first decimal ordering uses decimal RANGE boundaries
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id,
          count(*) OVER (
            ORDER BY CASE WHEN p THEN CAST(id AS INT) ELSE CAST(id AS DECIMAL(12,2)) END
            RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
          ) AS n,
          sum(id) OVER (
            ORDER BY CASE WHEN p THEN CAST(id AS INT) ELSE CAST(id AS DECIMAL(12,2)) END DESC
            RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
          ) AS s
        FROM VALUES (0, true), (1, false), (2, true), (3, false), (4, CAST(NULL AS BOOLEAN)) AS t(id, p)
        """
      Then query result collected
        | id | n | s |
        | 0  | 2 | 1 |
        | 1  | 3 | 3 |
        | 2  | 3 | 5 |
        | 3  | 3 | 7 |
        | 4  | 2 | 4 |

    Scenario: Sequence accepts a conditional BIGINT stop without a generator
      When query
        """
        SELECT n,
          typeof(sequence(0, CASE WHEN n <= 0 THEN 1 ELSE n END - 1)) AS result_type,
          sequence(0, CASE WHEN n <= 0 THEN 1 ELSE n END - 1) AS result
        FROM VALUES (3L), (1L), (5L) AS t(n)
        """
      Then query result collected
        | n | result_type   | result          |
        | 3 | array<bigint> | [0, 1, 2]       |
        | 1 | array<bigint> | [0]             |
        | 5 | array<bigint> | [0, 1, 2, 3, 4] |

    Scenario Outline: <generator> expands a sequence with a conditional BIGINT stop
      When query
        """
        SELECT n, <generator>(sequence(0, CASE WHEN n <= 0 THEN 1 ELSE n END - 1)) AS result
        FROM VALUES (3L), (1L), (5L) AS t(n)
        """
      Then query result collected
        | n | result |
        | 3 | 0      |
        | 3 | 1      |
        | 3 | 2      |
        | 1 | 0      |
        | 5 | 0      |
        | 5 | 1      |
        | 5 | 2      |
        | 5 | 3      |
        | 5 | 4      |

      Examples:
        | generator     |
        | explode       |
        | explode_outer |

    Scenario: Posexplode expands a sequence with a conditional BIGINT stop
      When query
        """
        SELECT n, posexplode(sequence(0, CASE WHEN n <= 0 THEN 1 ELSE n END - 1)) AS (pos, result)
        FROM VALUES (3L), (1L), (5L) AS t(n)
        """
      Then query result collected
        | n | pos | result |
        | 3 | 0   | 0      |
        | 3 | 1   | 1      |
        | 3 | 2   | 2      |
        | 1 | 0   | 0      |
        | 5 | 0   | 0      |
        | 5 | 1   | 1      |
        | 5 | 2   | 2      |
        | 5 | 3   | 3      |
        | 5 | 4   | 4      |

  Rule: Nested array consumers preserve previously matching conditional values

    Scenario Outline: Nested array conditionals retain matching values with <first_type> as the first result type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id,
          arrays_overlap(
            array(CASE WHEN p THEN <one> ELSE
              element_at(array_repeat(CASE WHEN q THEN 16777217L ELSE c END, 1), 1)
            END),
            array(16777217)) AS case_result,
          arrays_overlap(
            array(if(p, <one>, element_at(array_repeat(if(q, 16777217L, c), 1), 1))),
            array(16777217)) AS if_result
        FROM (
          SELECT id, p, q,
            CASE WHEN r THEN CAST(1 AS FLOAT) ELSE CAST(1 AS DECIMAL(8,2)) END AS c
          FROM VALUES
            (0, false, true, false),
            (1, true, true, false),
            (2, CAST(NULL AS BOOLEAN), true, false),
            (3, false, false, false)
          AS t(id, p, q, r)
        ) AS projected
        """
      Then query result collected
        | id | case_result | if_result |
        | 0  | true        | true      |
        | 1  | false       | false     |
        | 2  | true        | true      |
        | 3  | false       | false     |

      Examples:
        | first_type | one |
        | INT        | 1   |
        | BIGINT     | 1L  |


  Rule: Conditional observations preserve consumer values chosen during resolution

    Scenario: Nested integral conditionals expose their common type in either branch position
      When query
        """
        SELECT
          typeof(if(p, 0, if(q, 1, 2L))) AS nested_else,
          typeof(if(p, if(q, 1, 2L), 0)) AS nested_then,
          typeof(CASE WHEN p THEN 0 ELSE CASE WHEN q THEN 1 ELSE 2L END END) AS case_else
        FROM VALUES (false, false) t(p, q)
        """
      Then query result collected
        | nested_else | nested_then | case_else |
        | bigint      | bigint      | bigint    |

    Scenario Outline: <consumer> retains precise values from projected CASE and IF branches
      Given config spark.sql.ansi.enabled = false
      When query
        """
        WITH s AS (
          SELECT id, p, CASE WHEN q THEN CAST(1 AS FLOAT)
                            ELSE CAST(1 AS DECIMAL(8,2)) END AS c
          FROM VALUES (0, true, false), (1, false, false), (2, NULL, false) t(id, p, q)
        ), branches AS (
          SELECT id, CASE WHEN p THEN 16777217L ELSE c END AS a,
                     if(p, 16777217L, c) AS b
          FROM s
        )
        SELECT id, CAST(element_at(<case_array>, -1) AS DOUBLE) AS case_value,
                   CAST(element_at(<if_array>, -1) AS DOUBLE) AS if_value
        FROM branches
        """
      Then query result collected
        | id | case_value | if_value   |
        | 0  | 16777217.0 | 16777217.0 |
        | 1  | 1.0        | 1.0        |
        | 2  | 1.0        | 1.0        |

      Examples:
        | consumer     | case_array                      | if_array                        |
        | array_repeat | array_repeat(a, 1)              | array_repeat(b, 1)              |
        | array_append | array_append(array(0L), a)      | array_append(array(0L), b)      |
        | array_insert | array_insert(array(0L), 2, a)   | array_insert(array(0L), 2, b)   |
        | slice        | slice(array(a), 1, 1)           | slice(array(b), 1, 1)           |
        | flatten      | flatten(array(array(a)))        | flatten(array(array(b)))        |

    Scenario: Conditional array overlap preserves a previously distinct integral value
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id,
          arrays_overlap(array(CASE WHEN p THEN 16777217L ELSE c END), array(16777216L)) AS case_value,
          arrays_overlap(array(if(p, 16777217L, c)), array(16777216L)) AS if_value
        FROM (
          SELECT id, p, CASE WHEN q THEN CAST(1 AS FLOAT)
                            ELSE CAST(1 AS DECIMAL(8,2)) END AS c
          FROM VALUES (0, true, false), (1, false, false), (2, NULL, false) t(id, p, q)
        ) s
        """
      Then query result collected
        | id | case_value | if_value |
        | 0  | false      | false    |
        | 1  | false      | false    |
        | 2  | false      | false    |

    Scenario Outline: Conditional observations preserve unsigned shift width with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          shiftrightunsigned(CASE WHEN p THEN 0 ELSE c END, 1) AS case_value,
          shiftrightunsigned(if(p, 0, c), 1) AS if_value
        FROM (
          SELECT id, p, CASE WHEN q THEN -1L
                            ELSE CAST('-1' AS DECIMAL(38,18)) END AS c
          FROM VALUES (0, false, false), (1, true, false), (2, NULL, false) t(id, p, q)
        ) s
        """
      Then query result collected
        | id | case_value | if_value   |
        | 0  | 2147483647 | 2147483647 |
        | 1  | 0          | 0          |
        | 2  | 2147483647 | 2147483647 |

      Examples:
        | ansi  |
        | false |
        | true  |

    @sail-bug
    Scenario: Deferred projected conditional typeof supplies its common type to literal consumers
      When query
        """
        SELECT id, typeof(c) AS value_type,
          to_json(named_struct(typeof(c), c)) AS json_value
        FROM (
          SELECT id, if(p, 1, 2L) AS c
          FROM VALUES (0, true), (1, false), (2, NULL) t(id, p)
        ) s
        """
      Then query result collected
        | id | value_type | json_value   |
        | 0  | bigint     | {"bigint":1} |
        | 1  | bigint     | {"bigint":2} |
        | 2  | bigint     | {"bigint":2} |


    Scenario: Sequence widens conditional starts stops and steps after lambda analysis
      When query
        """
        SELECT id,
          sequence(CASE WHEN p THEN 0 ELSE 0L END, 2, 1) AS conditional_start,
          sequence(0, CASE WHEN p THEN 2 ELSE 2L END, 1) AS conditional_stop,
          sequence(0, 2, if(p, 1, 1L)) AS conditional_step
        FROM VALUES (0, true), (1, false), (2, NULL) t(id, p)
        """
      Then query result collected
        | id | conditional_start | conditional_stop | conditional_step |
        | 0  | [0, 1, 2]         | [0, 1, 2]        | [0, 1, 2]        |
        | 1  | [0, 1, 2]         | [0, 1, 2]        | [0, 1, 2]        |
        | 2  | [0, 1, 2]         | [0, 1, 2]        | [0, 1, 2]        |

    Scenario: Sequence preserves an earlier boundary error while recovering mixed-width arguments
      When query
        """
        SELECT sequence(0,
          CASE WHEN id = 0 THEN 2 ELSE CAST(raise_error('later-stop') AS BIGINT) END,
          -1) AS result
        FROM VALUES (0), (1) t(id)
        """
      Then query error Illegal sequence boundaries: 0 to 2 by -1

  Rule: Conditional observations retain previously correct producer types

    Scenario Outline: Previously correct conditional observations remain correct for <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        <query>
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case             | ansi  | query                                                                                                                                | result    |
        | CASE instr       | false | SELECT typeof(CASE WHEN false THEN 0 ELSE regexp_instr('aba', 'a') END) AS result                                                      | int       |
        | CASE instr       | true  | SELECT typeof(CASE WHEN false THEN 0 ELSE regexp_instr('aba', 'a') END) AS result                                                      | int       |
        | IF instr         | false | SELECT typeof(if(false, 0, regexp_instr('aba', 'a'))) AS result                                                                         | int       |
        | IF instr         | true  | SELECT typeof(if(false, 0, regexp_instr('aba', 'a'))) AS result                                                                         | int       |
        | CASE count       | false | SELECT typeof(CASE WHEN false THEN 0 ELSE regexp_count('aba', 'a') END) AS result                                                      | int       |
        | CASE count       | true  | SELECT typeof(CASE WHEN false THEN 0 ELSE regexp_count('aba', 'a') END) AS result                                                      | int       |
        | projected instr  | false | SELECT typeof(if(false, 0, c)) AS result FROM (SELECT regexp_instr('aba', 'a') AS c) s                                                  | int       |
        | projected instr  | true  | SELECT typeof(if(false, 0, c)) AS result FROM (SELECT regexp_instr('aba', 'a') AS c) s                                                  | int       |
        | JSON field name  | false | SELECT json_tuple('{"int":"ok","bigint":"changed"}', typeof(if(false, 0, regexp_count('aba', 'a')))) AS result                          | ok        |
        | JSON field name  | true  | SELECT json_tuple('{"int":"ok","bigint":"changed"}', typeof(if(false, 0, regexp_count('aba', 'a')))) AS result                          | ok        |
        | named struct key | false | SELECT to_json(named_struct(typeof(CASE WHEN false THEN 0 ELSE regexp_count('aba', 'a') END), 2)) AS result                            | {"int":2} |
        | named struct key | true  | SELECT to_json(named_struct(typeof(CASE WHEN false THEN 0 ELSE regexp_count('aba', 'a') END), 2)) AS result                            | {"int":2} |

    Scenario Outline: Explicitly cast projected regex results retain their BIGINT conditional type with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(if(false, 0, c)) AS result
        FROM (SELECT CAST(regexp_instr('aba', 'a') AS BIGINT) AS c) s
        """
      Then query result collected
        | result |
        | bigint |

      Examples:
        | ansi  |
        | false |
        | true  |
