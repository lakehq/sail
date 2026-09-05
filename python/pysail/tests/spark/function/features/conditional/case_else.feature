Feature: CASE ELSE values and nullability

  Rule: Explicit ELSE participates in branch coercion and schema nullability

    Scenario Outline: CASE ELSE preserves non-null numeric results with ANSI <ansi>: <type>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN <left> ELSE <right> END AS result,
               CASE WHEN p THEN <right> ELSE <left> END AS reversed
        FROM VALUES (true), (false), (CAST(NULL AS BOOLEAN)) AS t(p)
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = false)
         |-- reversed: <type> (nullable = false)
        """
      And query result collected
        | result        | reversed      |
        | <left_value>  | <right_value> |
        | <right_value> | <left_value>  |
        | <right_value> | <left_value>  |

      Examples:
        | ansi  | left | right       | left_value | right_value | type          |
        | false | 1    | 4294967296L | 1          | 4294967296  | long          |
        | true  | 1    | 4294967296L | 1          | 4294967296  | long          |
        | false | 1    | 1.5D        | 1.0        | 1.5         | double        |
        | true  | 1    | 1.5D        | 1.0        | 1.5         | double        |
        | false | 1    | 1.5BD       | 1.0        | 1.5         | decimal(11,1) |
        | true  | 1    | 1.5BD       | 1.0        | 1.5         | decimal(11,1) |
        | false | 1    | 1.5F        | 1.0        | 1.5         | float         |
        | true  | 1    | 1.5F        | 1.0        | 1.5         | double        |

    Scenario Outline: CASE ELSE retains nullable branches and omitted fallbacks with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN a ELSE 9L END AS nullable_then,
               CASE WHEN p THEN 8L ELSE b END AS nullable_else,
               CASE WHEN p THEN 6L END AS omitted_else,
               CASE WHEN p THEN 6L ELSE NULL END AS null_else
        FROM VALUES
          (true, NULL, 10),
          (true, 2, 20),
          (false, 3, NULL),
          (false, 4, 40),
          (NULL, 5, 50)
        AS t(p, a, b)
        """
      Then query schema
        """
        root
         |-- nullable_then: long (nullable = true)
         |-- nullable_else: long (nullable = true)
         |-- omitted_else: long (nullable = true)
         |-- null_else: long (nullable = true)
        """
      And query result collected
        | nullable_then | nullable_else | omitted_else | null_else |
        | NULL          | 8             | 6            | 6         |
        | 2             | 8             | 6            | 6         |
        | 9             | NULL          | NULL         | NULL      |
        | 9             | 40            | NULL         | NULL      |
        | 9             | 50            | NULL         | NULL      |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: CASE ELSE schema retains nullable values behind guards with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN n IS NOT NULL THEN n ELSE 0 END AS guarded,
               CASE WHEN false THEN CAST(NULL AS INT) ELSE 1 END AS unreachable_null
        FROM VALUES (CAST(NULL AS INT)), (1) AS t(n)
        """
      Then query schema
        """
        root
         |-- guarded: integer (nullable = true)
         |-- unreachable_null: integer (nullable = true)
        """
      And query result collected
        | guarded | unreachable_null |
        | 0       | 1                |
        | 1       | 1                |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: CASE ELSE schema includes cast nullability in both branch orders with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN CAST(s AS BIGINT) ELSE 0L END AS cast_then,
               CASE WHEN p THEN 0L ELSE CAST(s AS BIGINT) END AS cast_else,
               CASE WHEN p THEN CAST(n AS BIGINT) ELSE 0L END AS safe_numeric_cast
        FROM VALUES (true, '1', 1), (false, '2', 2) AS t(p, s, n)
        """
      Then query schema
        """
        root
         |-- cast_then: long (nullable = true)
         |-- cast_else: long (nullable = true)
         |-- safe_numeric_cast: long (nullable = false)
        """
      And query result collected
        | cast_then | cast_else | safe_numeric_cast |
        | 1         | 0         | 1                 |
        | 0         | 2         | 0                 |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: CASE ELSE distinguishes safe numeric casts from nullable decimal casts
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN p THEN CAST(n AS SMALLINT) ELSE CAST(0 AS SMALLINT) END AS narrowed_integer,
               CASE WHEN p THEN CAST(d AS DECIMAL(4,1)) ELSE 2.0BD END AS reduced_scale,
               CASE WHEN p THEN CAST(n AS DECIMAL(38,38))
                    ELSE CAST(0 AS DECIMAL(38,38)) END AS nullable_decimal
        FROM VALUES (true, 0, 1.20BD), (false, 0, 1.20BD) AS t(p, n, d)
        """
      Then query schema
        """
        root
         |-- narrowed_integer: short (nullable = false)
         |-- reduced_scale: decimal(4,1) (nullable = false)
         |-- nullable_decimal: decimal(38,38) (nullable = true)
        """
      And query result collected
        | narrowed_integer | reduced_scale | nullable_decimal |
        | 0                | 1.2           | 0E-38            |
        | 0                | 2.0           | 0E-38            |

    Scenario Outline: CASE ELSE includes decimal arithmetic nullability with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN id = 0 THEN wide + CAST(1 AS DECIMAL(20,0))
                    ELSE CAST(9 AS DECIMAL(21,0)) END AS add_then,
               CASE WHEN id = 0 THEN CAST(9 AS DECIMAL(21,0))
                    ELSE wide + CAST(1 AS DECIMAL(20,0)) END AS add_else,
               CASE WHEN id = 0 THEN wide - CAST(1 AS DECIMAL(20,0))
                    ELSE CAST(9 AS DECIMAL(21,0)) END AS subtract_then,
               CASE WHEN id = 0 THEN CAST(9 AS DECIMAL(21,0))
                    ELSE wide - CAST(1 AS DECIMAL(20,0)) END AS subtract_else,
               CASE WHEN id = 0 THEN narrow * CAST(3 AS DECIMAL(10,0))
                    ELSE CAST(9 AS DECIMAL(21,0)) END AS multiply_then,
               CASE WHEN id = 0 THEN CAST(9 AS DECIMAL(21,0))
                    ELSE narrow * CAST(3 AS DECIMAL(10,0)) END AS multiply_else
        FROM (
          SELECT id, CAST(id AS DECIMAL(20,0)) AS wide,
                 CAST(CAST(id AS INT) AS DECIMAL(10,0)) AS narrow
          FROM range(2)
        )
        """
      Then query schema
        """
        root
         |-- add_then: decimal(21,0) (nullable = <nullable>)
         |-- add_else: decimal(21,0) (nullable = <nullable>)
         |-- subtract_then: decimal(21,0) (nullable = <nullable>)
         |-- subtract_else: decimal(21,0) (nullable = <nullable>)
         |-- multiply_then: decimal(21,0) (nullable = <nullable>)
         |-- multiply_else: decimal(21,0) (nullable = <nullable>)
        """
      And query result collected
        | add_then | add_else | subtract_then | subtract_else | multiply_then | multiply_else |
        | 1        | 9        | -1            | 9             | 0             | 9             |
        | 9        | 2        | 9             | 0             | 9             | 3             |

      Examples:
        | ansi  | nullable |
        | false | true     |
        | true  | false    |

    Scenario: CASE ELSE recognizes non-null coalesce results enclosing nullable expressions
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN p THEN coalesce(CAST(s AS BIGINT), 0L) ELSE 0L END AS cast_result,
               CASE WHEN p THEN coalesce(n + CAST(1 AS DECIMAL(20,0)), CAST(0 AS DECIMAL(21,0)))
                    ELSE CAST(0 AS DECIMAL(21,0)) END AS arithmetic_result
        FROM VALUES (true, '1', CAST(1 AS DECIMAL(20,0))),
                    (false, '2', CAST(2 AS DECIMAL(20,0))) AS t(p, s, n)
        """
      Then query schema
        """
        root
         |-- cast_result: long (nullable = false)
         |-- arithmetic_result: decimal(21,0) (nullable = false)
        """
      And query result collected
        | cast_result | arithmetic_result |
        | 1           | 2                 |
        | 0           | 0                 |

    Scenario Outline: CASE ELSE follows nested coalesce nullability with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN coalesce(CAST(s AS BIGINT), 0L) ELSE 0L END AS cast_then,
               CASE WHEN p THEN 0L ELSE coalesce(CAST(s AS BIGINT), 0L) END AS cast_else,
               CASE WHEN p THEN coalesce(CAST(NULL AS BIGINT), coalesce(CAST(s AS BIGINT), 0L))
                    ELSE 0L END AS nested,
               CASE WHEN p THEN coalesce(CAST(s AS BIGINT), CAST('4' AS BIGINT))
                    ELSE 0L END AS nullable_inputs
        FROM VALUES (true, '1'), (true, CAST(NULL AS STRING)), (false, '2') AS t(p, s)
        """
      Then query schema
        """
        root
         |-- cast_then: long (nullable = false)
         |-- cast_else: long (nullable = false)
         |-- nested: long (nullable = false)
         |-- nullable_inputs: long (nullable = true)
        """
      And query result collected
        | cast_then | cast_else | nested | nullable_inputs |
        | 1         | 0         | 1      | 1               |
        | 0         | 0         | 0      | 4               |
        | 0         | 2         | 0      | 0               |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: CASE ELSE keeps all-null branches nullable with their common type
      When query
        """
        SELECT CASE WHEN p THEN NULL ELSE NULL END AS untyped,
               typeof(CASE WHEN p THEN NULL ELSE NULL END) AS untyped_type
        FROM VALUES (true), (false), (CAST(NULL AS BOOLEAN)) AS t(p)
        """
      Then query result collected
        | untyped | untyped_type |
        | NULL    | void         |
        | NULL    | void         |
        | NULL    | void         |
      When query
        """
        SELECT CASE WHEN p THEN NULL ELSE CAST(NULL AS BIGINT) END AS typed
        FROM VALUES (true), (false), (CAST(NULL AS BOOLEAN)) AS t(p)
        """
      Then query schema
        """
        root
         |-- typed: long (nullable = true)
        """
      And query result collected
        | typed |
        | NULL  |
        | NULL  |
        | NULL  |

    Scenario: CASE ELSE schema ignores branches after a literal true condition
      When query
        """
        SELECT CASE WHEN true THEN 1 END AS omitted,
               CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS explicit,
               CASE WHEN true THEN 1 WHEN false THEN CAST(NULL AS INT) ELSE 2 END AS later
        """
      Then query schema
        """
        root
         |-- omitted: integer (nullable = false)
         |-- explicit: integer (nullable = false)
         |-- later: integer (nullable = false)
        """
      And query result collected
        | omitted | explicit | later |
        | 1       | 1        | 1     |

    Scenario: CASE literal true retains nullable values within its reachable prefix
      When query
        """
        SELECT CASE WHEN n IS NOT NULL THEN n WHEN true THEN 0
                    ELSE CAST(NULL AS INT) END AS guarded_prefix,
               CASE WHEN false THEN CAST(NULL AS INT) WHEN true THEN 1
                    ELSE 2 END AS nullable_prefix,
               CASE WHEN true THEN CAST(NULL AS INT) WHEN true THEN 1
                    ELSE 2 END AS nullable_true,
               CASE WHEN p THEN 1 WHEN true THEN 2 WHEN true THEN CAST(NULL AS INT)
                    ELSE CAST(NULL AS INT) END AS first_true
        FROM VALUES (CAST(NULL AS INT), true), (1, false) AS t(n, p)
        """
      Then query schema
        """
        root
         |-- guarded_prefix: integer (nullable = true)
         |-- nullable_prefix: integer (nullable = true)
         |-- nullable_true: integer (nullable = true)
         |-- first_true: integer (nullable = false)
        """
      And query result collected
        | guarded_prefix | nullable_prefix | nullable_true | first_true |
        | 0              | 1               | NULL          | 1          |
        | 1              | 1               | NULL          | 2          |

    Scenario: CASE literal true still widens with all unreachable result branches
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN false THEN 4294967296L ELSE 2 END AS later_bigint,
               CASE WHEN true THEN 1 ELSE 1.5D END AS else_double,
               CASE WHEN true THEN 1 WHEN false THEN CAST(NULL AS DECIMAL(20,2))
                    ELSE 2 END AS later_decimal
        """
      Then query schema
        """
        root
         |-- later_bigint: long (nullable = false)
         |-- else_double: double (nullable = false)
         |-- later_decimal: decimal(20,2) (nullable = false)
        """
      And query result collected
        | later_bigint | else_double | later_decimal |
        | 1            | 1.0         | 1.00          |

    Scenario: CASE schema does not treat other constant conditions as literal true
      When query
        """
        SELECT CASE WHEN 1 = 1 THEN 1 END AS constant_condition,
               CASE true WHEN true THEN 1 END AS simple_case
        """
      Then query schema
        """
        root
         |-- constant_condition: integer (nullable = true)
         |-- simple_case: integer (nullable = true)
        """
      And query result collected
        | constant_condition | simple_case |
        | 1                  | 1           |

    Scenario: CASE literal true still validates later conditions
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN 42 THEN 2 ELSE 3 END
        """
      Then query error (?i)(boolean|bool|unexpected_input_type)

  Rule: Explicit ELSE preserves conditional execution

    Scenario Outline: CASE ELSE preserves first matches and nested branches with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN id >= 1 THEN 1 WHEN id >= 2 THEN 4294967296L ELSE 3 END AS first_match,
               CASE id WHEN 0 THEN 1 WHEN 1 THEN 4294967296L ELSE 3 END AS simple,
               CASE WHEN id = 0 THEN CASE id WHEN 0 THEN 1 ELSE 4294967296L END
                    ELSE CASE WHEN id = 1 THEN 2 ELSE 3 END END AS nested_searched,
               CASE id WHEN 0 THEN CASE WHEN id = 0 THEN 1 ELSE 2 END
                    ELSE CASE id WHEN 1 THEN 4294967296L ELSE 3 END END AS nested_simple
        FROM VALUES (0), (1), (2), (CAST(NULL AS INT)) AS t(id)
        """
      Then query schema
        """
        root
         |-- first_match: long (nullable = false)
         |-- simple: long (nullable = false)
         |-- nested_searched: long (nullable = false)
         |-- nested_simple: long (nullable = false)
        """
      And query result collected
        | first_match | simple     | nested_searched | nested_simple |
        | 3           | 1          | 1               | 1             |
        | 1           | 4294967296 | 2               | 4294967296    |
        | 1           | 3          | 3               | 3             |
        | 3           | 3          | 3               | 3             |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: CASE ELSE evaluates division only in the selected branch with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN id = 0 THEN 7 ELSE 20 DIV id END AS guarded_else,
               CASE WHEN id <> 0 THEN 20 DIV id ELSE 7 END AS guarded_then
        FROM range(3)
        """
      Then query result collected
        | guarded_else | guarded_then |
        | 7            | 7            |
        | 20           | 20           |
        | 10           | 10           |

      Examples:
        | ansi  |
        | false |
        | true  |
