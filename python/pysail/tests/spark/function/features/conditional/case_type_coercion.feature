Feature: Conditional branch type coercion

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
        | INT          | DECIMAL(20,0) | decimal(20,0) |
        | DECIMAL(8,2) | DECIMAL(10,4) | decimal(10,4) |
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
        | DECIMAL(10,2) | 1.75       | decimal(12,2) | 1.00 |

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
