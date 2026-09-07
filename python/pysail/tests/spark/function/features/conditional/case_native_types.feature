@sail-only
Feature: Conditional branch coercion for Sail native numeric types

  # Spark has no unsigned integer types. These scenarios preserve Sail's native
  # numeric coercion and full-range values through the conditional type hint.

  Rule: Native unsigned branches expose the analyzer's common type

    Scenario Outline: Native <left_type> and <right_type> branches preserve boundary values
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          id,
          typeof(CASE WHEN p THEN l ELSE r END) AS case_type,
          typeof(if(p, r, l)) AS reversed_type,
          CAST(CASE WHEN p THEN l ELSE r END AS STRING) AS case_value,
          CAST(if(p, l, r) AS STRING) AS if_value,
          CAST(CASE WHEN p THEN r ELSE l END AS STRING) AS reversed_case,
          CAST(if(p, r, l) AS STRING) AS reversed_if
        FROM (
          SELECT id, p,
            CAST(<left_literal> AS <left_type>) AS l,
            CAST(<right_literal> AS <right_type>) AS r
          FROM VALUES (0, true), (1, false), (2, NULL) AS t(id, p)
        ) AS inputs
        """
      Then query result collected
        | id | case_type     | reversed_type | case_value    | if_value      | reversed_case | reversed_if   |
        | 0  | <common_type> | <common_type> | <left_value>  | <left_value>  | <right_value> | <right_value> |
        | 1  | <common_type> | <common_type> | <right_value> | <right_value> | <left_value>  | <left_value>  |
        | 2  | <common_type> | <common_type> | <right_value> | <right_value> | <left_value>  | <left_value>  |

      Examples:
        | left_type | right_type | left_literal           | right_literal | common_type       | left_value            | right_value |
        | UINT8     | UINT16     | 255                    | 65535         | unsigned smallint | 255                   | 65535       |
        | UINT16    | SMALLINT   | 65535                  | -32768        | int               | 65535                 | -32768      |
        | UINT32    | INT        | 4294967295             | -2147483648   | bigint            | 4294967295            | -2147483648 |
        | UINT64    | UINT32     | '18446744073709551615' | 4294967295    | unsigned bigint   | 18446744073709551615  | 4294967295  |
        | UINT32    | FLOAT      | 4294967295             | 1.5           | float             | 4294967300.0          | 1.5         |
        | UINT64    | DOUBLE     | '18446744073709551615' | 1.5           | double            | 1.8446744073709552e19 | 1.5         |

    Scenario Outline: NULL branches preserve native <native_type> values and type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          typeof(CASE WHEN p THEN NULL WHEN q THEN v END) AS case_type,
          typeof(if(p, NULL, if(q, v, NULL))) AS if_type,
          CAST(CASE WHEN p THEN NULL WHEN q THEN v END AS STRING) AS case_value,
          CAST(if(p, NULL, if(q, v, NULL)) AS STRING) AS if_value
        FROM (
          SELECT id, p, q, CAST(<literal> AS <native_type>) AS v
          FROM VALUES (0, true, true), (1, false, true),
                      (2, false, false), (3, NULL, NULL) AS t(id, p, q)
        ) AS inputs
        """
      Then query result collected
        | id | case_type   | if_type     | case_value | if_value |
        | 0  | <type_name> | <type_name> | NULL       | NULL     |
        | 1  | <type_name> | <type_name> | <value>    | <value>  |
        | 2  | <type_name> | <type_name> | NULL       | NULL     |
        | 3  | <type_name> | <type_name> | NULL       | NULL     |

      Examples:
        | native_type | literal                | type_name         | value                |
        | UINT8       | 255                    | unsigned tinyint  | 255                  |
        | UINT16      | 65535                  | unsigned smallint | 65535                |
        | UINT32      | 4294967295             | unsigned int      | 4294967295           |
        | UINT64      | '18446744073709551615' | unsigned bigint   | 18446744073709551615 |

    Scenario Outline: Projected mixed signedness branches preserve widened values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          typeof(CASE WHEN p THEN CAST(127 AS TINYINT) ELSE branch END) AS case_type,
          typeof(if(p, CAST(127 AS TINYINT), branch)) AS if_type,
          CAST(CASE WHEN p THEN CAST(127 AS TINYINT) ELSE branch END AS STRING) AS case_value,
          CAST(if(p, CAST(127 AS TINYINT), branch) AS STRING) AS if_value
        FROM (
          SELECT id, p,
            CASE WHEN q THEN CAST(4294967295 AS UINT32)
                 ELSE CAST(-2147483648 AS INT) END AS branch
          FROM VALUES (0, true, true), (1, false, true),
                      (2, false, false), (3, NULL, NULL) AS t(id, p, q)
        ) AS inputs
        """
      Then query result collected
        | id | case_type | if_type | case_value  | if_value    |
        | 0  | bigint    | bigint  | 127         | 127         |
        | 1  | bigint    | bigint  | 4294967295  | 4294967295  |
        | 2  | bigint    | bigint  | -2147483648 | -2147483648 |
        | 3  | bigint    | bigint  | -2147483648 | -2147483648 |

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: Spark producer observations preserve native parent coercion

    Scenario Outline: Native <native_type> parents preserve nested and projected branch widths with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          typeof(if(p, n, regexp_count('aba', 'a'))) AS direct_type,
          typeof(if(p, n, if(false, 0, regexp_count('aba', 'a')))) AS nested_type,
          typeof(if(p, n, c)) AS projected_type,
          typeof(if(p, n, CAST(c AS BIGINT))) AS bigint_cast_type,
          typeof(if(p, n, CAST(c AS INT))) AS int_cast_type,
          CAST(if(p, n, regexp_count('aba', 'a')) AS STRING) AS direct_value,
          CAST(if(p, n, if(false, 0, regexp_count('aba', 'a'))) AS STRING) AS nested_value,
          CAST(if(p, n, c) AS STRING) AS projected_value
        FROM (
          SELECT id, p, CAST(<maximum> AS <native_type>) AS n,
            if(false, 0, regexp_count('aba', 'a')) AS c
          FROM VALUES (0, true), (1, false), (2, NULL) t(id, p)
        ) s
        """
      Then query result collected
        | id | direct_type | nested_type | projected_type | bigint_cast_type | int_cast_type | direct_value | nested_value | projected_value |
        | 0  | bigint      | bigint      | int            | bigint           | int           | <maximum>    | <maximum>    | <maximum>       |
        | 1  | bigint      | bigint      | int            | bigint           | int           | 2            | 2            | 2               |
        | 2  | bigint      | bigint      | int            | bigint           | int           | 2            | 2            | 2               |

      Examples:
        | native_type | maximum | ansi  |
        | UINT8       | 255     | false |
        | UINT8       | 255     | true  |
        | UINT16      | 65535   | false |
        | UINT16      | 65535   | true  |

    Scenario Outline: Native <native_type> lambda elements do not affect index or accumulator observations with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT transform(array(n), (x, i) -> named_struct(
          'element_type', typeof(if(false, x, if(false, 0, regexp_count('aba', 'a')))),
          'index_type', typeof(if(false, i, if(false, 0, regexp_count('aba', 'a'))))
        )) AS parameter_types,
          aggregate(array(n), 0, (acc, x) -> acc,
            acc -> typeof(if(false, acc, if(false, 0, regexp_count('aba', 'a'))))) AS finish_type,
          aggregate(array(0), if(false, 0, n), (acc, x) -> acc,
            acc -> typeof(if(false, acc, if(false, 0, regexp_count('aba', 'a'))))) AS native_finish_type
        FROM (SELECT CAST(0 AS <native_type>) AS n) producer
        """
      Then query result collected
        | parameter_types                                      | finish_type | native_finish_type |
        | [Row(element_type='bigint', index_type='int')]         | int         | bigint             |

      Examples:
        | native_type | ansi  |
        | UINT8       | false |
        | UINT8       | true  |
        | UINT16      | false |
        | UINT16      | true  |

  Rule: Native Decimal256 type hints preserve existing division behavior

    Scenario Outline: Nested and projected Decimal256 branches preserve DOUBLE division with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          typeof((CASE WHEN p THEN 1
                       ELSE CASE WHEN q THEN CAST('1.25' AS DECIMAL(50,10)) ELSE 1.5D END
                  END) / 3) AS case_type,
          typeof(if(p, 1, branch) / 3) AS if_type,
          CAST((CASE WHEN p THEN 1
                     ELSE CASE WHEN q THEN CAST('1.25' AS DECIMAL(50,10)) ELSE 1.5D END
                END) / 3 AS STRING) AS case_value,
          CAST(if(p, 1, branch) / 3 AS STRING) AS if_value
        FROM (
          SELECT id, p, q,
            if(q, CAST('1.25' AS DECIMAL(50,10)), 1.5D) AS branch
          FROM VALUES (0, true, true), (1, true, false), (2, false, true),
                      (3, false, false), (4, NULL, NULL) AS t(id, p, q)
        ) AS inputs
        """
      Then query result collected
        | id | case_type | if_type | case_value         | if_value           |
        | 0  | double    | double  | 0.3333333333333333 | 0.3333333333333333 |
        | 1  | double    | double  | 0.3333333333333333 | 0.3333333333333333 |
        | 2  | double    | double  | 0.4166666666666667 | 0.4166666666666667 |
        | 3  | double    | double  | 0.5                | 0.5                |
        | 4  | double    | double  | 0.5                | 0.5                |

      Examples:
        | ansi  |
        | false |
        | true  |

  Rule: Native conditional windows preserve existing value conversions

    # Spark BIGINT controls verify the logical values; Spark has no unsigned types.

    Scenario Outline: Inline unsigned <conditional> preserves large <window> values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          CAST(<window>(<branch>) OVER (ORDER BY id) AS BIGINT) AS value
        FROM VALUES (0, true), (1, false), (2, true) AS t(id, p)
        """
      Then query result collected
        | id | value      |
        | 0  | <first>    |
        | 1  | 3000000000 |
        | 2  | <last>     |

      Examples:
        | conditional | window | branch                                                                 | first | last | ansi  |
        | CASE        | lag    | CASE WHEN p THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END | NULL  | 1    | false |
        | CASE        | lead   | CASE WHEN p THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END | 1     | NULL | false |
        | IF          | lag    | if(p, CAST(3000000000 AS UINT32), CAST(1 AS UINT64))                   | NULL  | 1    | false |
        | IF          | lead   | if(p, CAST(3000000000 AS UINT32), CAST(1 AS UINT64))                   | 1     | NULL | false |
        | CASE        | lag    | CASE WHEN p THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END | NULL  | 1    | true  |
        | CASE        | lead   | CASE WHEN p THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END | 1     | NULL | true  |
        | IF          | lag    | if(p, CAST(3000000000 AS UINT32), CAST(1 AS UINT64))                   | NULL  | 1    | true  |
        | IF          | lead   | if(p, CAST(3000000000 AS UINT32), CAST(1 AS UINT64))                   | 1     | NULL | true  |

    Scenario Outline: Existing UInt64 windows retain unsigned shift behavior with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          shiftrightunsigned(lag(CAST(3 AS UINT64)) OVER (ORDER BY id), 1) AS raw_lag,
          shiftrightunsigned(lead(CAST(3 AS UINT64)) OVER (ORDER BY id), 1) AS raw_lead,
          shiftrightunsigned(lag(CASE WHEN p THEN CAST(3 AS UINT64)
                                     ELSE CAST(5 AS UINT32) END)
                             OVER (ORDER BY id), 1) AS case_lag,
          shiftrightunsigned(lead(CASE WHEN p THEN CAST(3 AS UINT64)
                                      ELSE CAST(5 AS UINT32) END)
                             OVER (ORDER BY id), 1) AS case_lead
        FROM VALUES (0, true), (1, false), (2, true) AS t(id, p)
        """
      Then query result collected
        | id | raw_lag | raw_lead | case_lag | case_lead |
        | 0  | NULL    | 1        | NULL     | 2         |
        | 1  | 1       | 1        | 1        | 1         |
        | 2  | 1       | NULL     | 2        | NULL      |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: A projected unsigned branch does not narrow DOUBLE <conditional> windows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH s AS (
          SELECT id, p,
            CASE WHEN q THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END AS c
          FROM VALUES (0, true, true), (1, true, false), (2, true, true) AS t(id, p, q)
        )
        SELECT id,
          CAST(lag(<branch>) OVER (ORDER BY id) AS DOUBLE) AS value
        FROM s
        """
      Then query result collected
        | id | value        |
        | 0  | NULL         |
        | 1  | 3000000000.0 |
        | 2  | 1.0          |

      Examples:
        | conditional | branch                                        | ansi  |
        | CASE        | CASE WHEN p THEN c ELSE CAST(2 AS DOUBLE) END | false |
        | IF          | if(p, c, CAST(2 AS DOUBLE))                   | false |
        | CASE        | CASE WHEN p THEN c ELSE CAST(2 AS DOUBLE) END | true  |
        | IF          | if(p, c, CAST(2 AS DOUBLE))                   | true  |

    Scenario Outline: A conditional window preserves array repeat and append values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id,
          array_append(
            array_repeat(
              lag(CASE WHEN p THEN CAST(n AS UINT32) ELSE CAST(n AS UINT64) END, 0)
                OVER (ORDER BY id),
              1),
            0) AS value
        FROM VALUES (0, true, 3), (1, false, 127), (2, true, 2147483647) AS t(id, p, n)
        """
      Then query result collected
        | id | value           |
        | 0  | [3, 0]          |
        | 1  | [127, 0]        |
        | 2  | [2147483647, 0] |

      Examples:
        | ansi |
        | true |


    Scenario Outline: Projected unsigned conditionals retain their full range in windows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH s AS (
          SELECT id,
            CASE WHEN p THEN CAST(3000000000 AS UINT32) ELSE CAST(1 AS UINT64) END AS a,
            if(p, CAST(3000000000 AS UINT32), CAST(1 AS UINT64)) AS b
          FROM VALUES (0, true), (1, false), (2, true) t(id, p)
        )
        SELECT id,
          CAST(lag(a) OVER (ORDER BY id) AS BIGINT) AS case_lag,
          CAST(lag(b) OVER (ORDER BY id) AS BIGINT) AS if_lag,
          CAST(lead(a) OVER (ORDER BY id) AS BIGINT) AS case_lead,
          CAST(lead(b) OVER (ORDER BY id) AS BIGINT) AS if_lead
        FROM s
        """
      Then query result collected
        | id | case_lag   | if_lag     | case_lead  | if_lead    |
        | 0  | NULL       | NULL       | 1          | 1          |
        | 1  | 3000000000 | 3000000000 | 3000000000 | 3000000000 |
        | 2  | 1          | 1          | NULL       | NULL       |

      Examples:
        | ansi  |
        | false |
        | true  |

  # Spark has no unsigned integer type. The signed TINYINT controls were
  # verified on Spark; these cases preserve the preceding Sail patch's results.

  Scenario Outline: Native array elements do not change index-only observations with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        transform(array(CAST(9 AS UINT8), CAST(8 AS UINT8)),
          (x, i) -> if(false, 0, i + regexp_instr('aba', 'a'))) AS value,
        typeof(transform(array(CAST(9 AS UINT8), CAST(8 AS UINT8)),
          (x, i) -> if(false, 0, i + regexp_instr('aba', 'a')))) AS observed_type
      """
    Then query result collected
      | value  | observed_type |
      | [1, 2] | array<int>    |

    Examples:
      | ansi  |
      | false |
      | true  |

  Scenario Outline: Native lambda observations preserve their declared field boundary for <element> with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT
        transform(array(<element>),
          x -> if(false, 0, x + regexp_instr('aba', 'a'))) AS value,
        typeof(transform(array(<element>),
          x -> if(false, 0, x + regexp_instr('aba', 'a')))) AS observed_type
      """
    Then query result collected
      | value   | observed_type   |
      | <value> | <observed_type> |

    Examples:
      | element                                | value | observed_type | ansi  |
      | CAST(2 AS UINT8)                        | [3]   | array<bigint> | false |
      | CAST(2 AS UINT8)                        | [3]   | array<bigint> | true  |
      | CAST(2 AS UINT8) + CAST(1 AS SMALLINT) | [4]   | array<int>    | false |
      | CAST(2 AS UINT8) + CAST(1 AS SMALLINT) | [4]   | array<int>    | true  |
