Feature: CASE result expression nullability

  Rule: CASE propagates each result expression's nullability contract

    Scenario Outline: CASE null tests consume nullable cast results with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN CAST(s AS BIGINT) IS NULL ELSE false END AS is_null,
               CASE WHEN p THEN CAST(s AS BIGINT) IS NOT NULL ELSE false END AS is_not_null,
               CASE WHEN p THEN TRY_CAST(broken AS BIGINT) IS NULL ELSE false END AS failed_cast
        FROM VALUES (true, '1', 'bad'),
                    (true, CAST(NULL AS STRING), '2'),
                    (false, '3', 'bad') AS t(p, s, broken)
        """
      Then query schema
        """
        root
         |-- is_null: boolean (nullable = false)
         |-- is_not_null: boolean (nullable = false)
         |-- failed_cast: boolean (nullable = false)
        """
      And query result collected
        | is_null | is_not_null | failed_cast |
        | false   | true        | true        |
        | true    | false       | false       |
        | false   | false       | false       |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: CASE null-safe equality remains non-nullable with nullable operands
      When query
        """
        SELECT CASE WHEN p THEN n <=> 1 ELSE false END AS equal_value,
               CASE WHEN p THEN n <=> NULL ELSE false END AS equal_null
        FROM VALUES (true, CAST(NULL AS INT)), (true, 1), (false, CAST(NULL AS INT)) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- equal_value: boolean (nullable = false)
         |-- equal_null: boolean (nullable = false)
        """
      And query result collected
        | equal_value | equal_null |
        | false       | true       |
        | true        | false      |
        | false       | false      |

    Scenario Outline: CASE null-consuming functions use corrected argument nullability with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN coalesce(CAST(s AS BIGINT), TRY_CAST(n AS BIGINT))
                    ELSE 0L END AS coalesced,
               CASE WHEN p THEN nvl(CAST(s AS BIGINT), 0L) ELSE 0L END AS defaulted
        FROM VALUES (true, CAST(NULL AS STRING), 5), (true, '2', 6), (false, '3', 7) AS t(p, s, n)
        """
      Then query schema
        """
        root
         |-- coalesced: long (nullable = false)
         |-- defaulted: long (nullable = false)
        """
      And query result collected
        | coalesced | defaulted |
        | 5         | 0         |
        | 2         | 2         |
        | 0         | 0         |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: CASE retains both nested IF result branches in nullability
      When query
        """
        SELECT CASE WHEN p THEN IF(true, 1, CAST(NULL AS INT)) ELSE 0 END AS nullable_false_arm,
               CASE WHEN p THEN IF(false, CAST(NULL AS INT), 1) ELSE 0 END AS nullable_true_arm,
               CASE WHEN p THEN IF(n IS NOT NULL, n, 0) ELSE 0 END AS guarded_arm,
               CASE WHEN p THEN IF(true, 1, 2) ELSE 0 END AS non_null_arms
        FROM VALUES (true, CAST(NULL AS INT)), (true, 1), (false, CAST(NULL AS INT)) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- nullable_false_arm: integer (nullable = true)
         |-- nullable_true_arm: integer (nullable = true)
         |-- guarded_arm: integer (nullable = true)
         |-- non_null_arms: integer (nullable = false)
        """
      And query result collected
        | nullable_false_arm | nullable_true_arm | guarded_arm | non_null_arms |
        | 1                  | 1                 | 0           | 1             |
        | 1                  | 1                 | 1           | 1             |
        | 0                  | 0                 | 0           | 0             |

    # TODO: named_struct marks every child field nullable in DataFusion, including
    # outside CASE. Preserve that constructor limitation until its metadata is fixed.
    @sail-bug
    Scenario: CASE named_struct results preserve non-null child fields
      When query
        """
        SELECT CASE WHEN p THEN named_struct('value', n)
                    ELSE named_struct('value', 0) END AS result
        FROM VALUES (true, 1), (false, 2) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- value: integer (nullable = false)
        """
      And query result collected
        | result       |
        | Row(value=1) |
        | Row(value=0) |

  Rule: CASE distinguishes safe TRY_CAST widening from nullable conversions

    Scenario Outline: CASE TRY_CAST from <input> to <target> has nullable <nullable>
      When query
        """
        SELECT CASE WHEN p THEN TRY_CAST(n AS <target>) ELSE <zero> END AS result
        FROM VALUES (true, <input>), (false, <input>) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = <nullable>)
        """
      And query result collected
        | result  |
        | <value> |
        | <zero_value> |

      Examples:
        | input  | target       | zero   | type         | nullable | value | zero_value |
        | 1      | BIGINT       | 0L     | long         | false    | 1     | 0          |
        | 1L     | INT          | 0      | integer      | true     | 1     | 0          |
        | 1234BD | BIGINT       | 0L     | long         | true     | 1234  | 0          |
        | 1      | DECIMAL(12,2)| 0.00BD | decimal(12,2)| false    | 1.00  | 0.00       |
        | 1.0F   | DOUBLE       | 0.0D   | double       | false    | 1.0   | 0.0        |
        | 1.0D   | FLOAT        | 0.0F   | float        | true     | 1.0   | 0.0        |
        | 1.0BD  | DOUBLE       | 0.0D   | double       | true     | 1.0   | 0.0        |
        | 1.0BD  | FLOAT        | 0.0F   | float        | true     | 1.0   | 0.0        |
        | 1.00BD | DECIMAL(6,2) | 0.00BD | decimal(6,2) | false    | 1.00  | 0.00       |
        | 1.00BD | DECIMAL(6,1) | 0.0BD  | decimal(6,1) | true     | 1.0   | 0.0        |

    Scenario: CASE widening TRY_CAST retains nullable source values
      When query
        """
        SELECT CASE WHEN p THEN TRY_CAST(n AS BIGINT) ELSE 0L END AS result
        FROM VALUES (true, CAST(NULL AS INT)), (true, 1), (false, 2) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """
      And query result collected
        | result |
        | NULL   |
        | 1      |
        | 0      |

    Scenario Outline: CASE nested TRY_CAST uses strict decimal integral widening with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN p THEN TRY_CAST(array(CAST(n AS DECIMAL(5,0))) AS ARRAY<SMALLINT>)
                    ELSE array(CAST(0 AS SMALLINT)) END AS equal_precision,
               CASE WHEN p THEN TRY_CAST(array(CAST(n AS DECIMAL(4,0))) AS ARRAY<SMALLINT>)
                    ELSE array(CAST(0 AS SMALLINT)) END AS lower_precision
        FROM VALUES (true, 1), (false, 2) AS t(p, n)
        """
      Then query schema
        """
        root
         |-- equal_precision: array (nullable = true)
         |    |-- element: short (containsNull = true)
         |-- lower_precision: array (nullable = false)
         |    |-- element: short (containsNull = true)
        """
      And query result collected
        | equal_precision | lower_precision |
        | [1]             | [1]             |
        | [0]             | [0]             |

      Examples:
        | ansi  |
        | false |
        | true  |
