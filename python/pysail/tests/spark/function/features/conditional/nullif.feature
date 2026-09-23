Feature: nullif output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(2, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullif stays nullable
      When query
        """
        SELECT nullif(c, 2) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a bigint second argument does not widen the declared type
      When query
        """
        SELECT nullif(CAST(1 AS INT), CAST(2 AS BIGINT)) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # A column, so the declared type cannot come from folding a literal.
    @sail-bug
    Scenario: a non-null column against a bigint keeps its declared type
      When query
        """
        SELECT nullif(CAST(id AS INT), CAST(2 AS BIGINT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a nullable column against a bigint keeps its declared type
      When query
        """
        SELECT nullif(c, CAST(2 AS BIGINT)) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # `typeof` shows the type but never the nullable flag, so the decimal case needs the schema too.
    @sail-bug
    Scenario: a decimal keeps its own precision and scale
      When query
        """
        SELECT nullif(CAST(1 AS DECIMAL(10,2)), CAST(2 AS DECIMAL(20,10))) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(10,2) (nullable = true)
        """

    # The untyped NULL literal: Spark keeps `left`'s type here too, and `left` is NullType.
    @sail-bug
    Scenario: an untyped NULL first argument declares void
      When query
        """
        SELECT nullif(NULL, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

    @sail-bug
    Scenario: two untyped NULL arguments declare void
      When query
        """
        SELECT nullif(NULL, NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: void (nullable = true)
        """

  # `NullIf` expands to `If(EqualTo(l, r), TypedNullLiteral(l), l)`
  # (`nullExpressions.scala:173-192`), so both surviving branches are `l` and the result carries
  # `l`'s type; only the comparison sees the wider type. Spark pins this in its own corpus:
  # `SELECT nullif(1, 2.1d)` has schema `struct<nullif(1, 2.1):int>`.
  # Types and values captured from Spark JVM 4.2.0, timezone UTC.
  Rule: The result keeps the first argument's type

    Scenario Outline: nullif of <left> against <right> types as <result>
      When query
        """
        SELECT typeof(nullif(CAST(<lv> AS <left>), CAST(2 AS <right>))) AS t,
               CAST(nullif(CAST(<lv> AS <left>), CAST(2 AS <right>)) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | left          | lv  | right          | result        | value |
        | BIGINT        | 1   | INT            | bigint        | 1     |

    @sail-bug
    Scenario Outline: nullif of <left> against <right> types as <result> (known Sail bug)
      When query
        """
        SELECT typeof(nullif(CAST(<lv> AS <left>), CAST(2 AS <right>))) AS t,
               CAST(nullif(CAST(<lv> AS <left>), CAST(2 AS <right>)) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | left          | lv  | right          | result        | value |
        | INT           | 1   | BIGINT         | int           | 1     |
        | TINYINT       | 1   | INT            | tinyint       | 1     |
        | INT           | 1   | DECIMAL(20,10) | int           | 1     |
        | DECIMAL(10,2) | 1   | DECIMAL(20,10) | decimal(10,2) | 1.00  |
        | FLOAT         | 0.1 | DOUBLE         | float         | 0.1   |

    # The equal branch returns NULL, and the NULL is still typed from the first argument.
    @sail-bug
    Scenario: nullif of equal arguments keeps the first argument's type
      When query
        """
        SELECT typeof(nullif(CAST(2 AS INT), CAST(2 AS BIGINT))) AS t,
               CAST(nullif(CAST(2 AS INT), CAST(2 AS BIGINT)) AS STRING) AS v
        """
      Then query result
        | t   | v    |
        | int | NULL |

    # A column first argument: plan-builder rewrites that only match `Expr::Literal` would miss it.
    @sail-bug
    Scenario: nullif of an int column against a bigint literal types as int
      When query
        """
        SELECT typeof(nullif(c, CAST(2 AS BIGINT))) AS t,
               CAST(nullif(c, CAST(2 AS BIGINT)) AS STRING) AS v
        FROM VALUES (CAST(1 AS INT)) AS t(c)
        """
      Then query result
        | t   | v |
        | int | 1 |

  # 2^53+1 is not representable as a DOUBLE, so it only survives if the value never travels
  # through the comparison type. A small value like `1` passes either way and would discriminate
  # nothing. Note the int-against-FLOAT pair is NOT a precision case here: under ANSI (the
  # default) Spark widens it to DOUBLE, which is what the ANSI rule at the end of this file pins.
  Rule: A boundary value survives the comparison type

    @sail-bug
    Scenario Outline: nullif of <case> against <right> keeps the exact value
      When query
        """
        SELECT typeof(nullif(CAST(<lv> AS <left>), CAST(1 AS <right>))) AS t,
               CAST(nullif(CAST(<lv> AS <left>), CAST(1 AS <right>)) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case               | left   | lv               | right  | result | value            |
        | 2^53+1 past double | BIGINT | 9007199254740993 | DOUBLE | bigint | 9007199254740993 |

    # Values a wider fixed-point type would flatten to zero, plus the specials. Asserted by
    # EQUALITY, not by rendering: Sail prints doubles differently from Java, which the next rule
    # covers on its own. Raw bits would be stronger but Spark rejects `CAST(<double> AS BINARY)`,
    # so equality is the ceiling.
    Scenario Outline: nullif of <case> keeps its own type and exact value
      When query
        """
        SELECT typeof(nullif(<lv>, CAST(1 AS BIGINT))) AS t,
               nullif(<lv>, CAST(1 AS BIGINT)) = <lv> AS same
        """
      Then query result
        | t        | same |
        | <result> | true |

      Examples:
        | case                      | lv                       | result |
        | smallest double subnormal | CAST(4.9E-324 AS DOUBLE) | double |
        | smallest float subnormal  | CAST(1.4E-45 AS FLOAT)   | float  |
        | NaN                       | CAST('NaN' AS DOUBLE)    | double |

    # As doubles, 2^53+1 and 2^53 are the same number. This pins that the COMPARISON still uses the
    # wider type: a fix that narrowed the comparison instead of the result would return the value.
    @sail-bug
    Scenario: nullif returns NULL when the operands are equal only after widening
      When query
        """
        SELECT typeof(nullif(CAST(9007199254740993 AS BIGINT), CAST(9007199254740992 AS DOUBLE))) AS t,
               CAST(nullif(CAST(9007199254740993 AS BIGINT), CAST(9007199254740992 AS DOUBLE)) AS STRING) AS v
        """
      Then query result
        | t      | v    |
        | bigint | NULL |

    # Through a column and several rows, so the fix cannot be literal-only. Row 2 is equal after
    # widening and must come back NULL.
    @sail-bug
    Scenario: nullif keeps boundary values coming from a column
      When query
        """
        SELECT CAST(nullif(a, b) AS STRING) AS v
        FROM VALUES
          (CAST(9007199254740993 AS BIGINT), CAST(1.0 AS DOUBLE)),
          (CAST(16777217 AS BIGINT), CAST(16777217 AS DOUBLE)),
          (CAST(NULL AS BIGINT), CAST(1.0 AS DOUBLE))
        AS t(a, b)
        ORDER BY v
        """
      Then query result ordered
        | v                |
        | NULL             |
        | NULL             |
        | 9007199254740993 |

  # Same values as the rule above, asserted by RENDERING. The results are already bit-correct;
  # only how Sail prints a double still differs, and it reproduces without `nullif`:
  # `SELECT CAST(CAST(4.9E-324 AS DOUBLE) AS STRING)` is `4.9E-324` in Spark, `5e-324` in Sail.
  @sail-bug
  Rule: Only the rendering of these values still differs

    Scenario Outline: nullif of <case> renders the way Spark renders it
      When query
        """
        SELECT CAST(nullif(<lv>, CAST(1 AS BIGINT)) AS STRING) AS v
        """
      Then query result
        | v       |
        | <value> |

      Examples:
        | case                      | lv                          | value     |
        | smallest double subnormal | CAST(4.9E-324 AS DOUBLE)    | 4.9E-324  |
        | smallest float subnormal  | CAST(1.4E-45 AS FLOAT)      | 1.4E-45   |
        | 1e-300                    | CAST(1E-300 AS DOUBLE)      | 1.0E-300  |
        | positive infinity         | CAST('Infinity' AS DOUBLE)  | Infinity  |
        | negative infinity         | CAST('-Infinity' AS DOUBLE) | -Infinity |

  # `OrderUtils.isOrderable` (`sql/api/.../types/OrderUtils.scala`) recurses into arrays and
  # structs, so both are valid arguments; maps are the type it rejects. `EqualTo` checks it in
  # `checkInputDataTypes` via `TypeUtils.checkForOrderingExpr`, so Spark refuses during ANALYSIS.
  #
  # The accepted half is pinned next to the rejected one on purpose: a rule made only of
  # rejections is satisfied by an implementation that rejects everything.
  Rule: Orderable arguments are accepted and maps are rejected

    Scenario Outline: nullif over <case>
      When query
        """
        SELECT typeof(nullif(<left>, <right>)) AS t, CAST(nullif(<left>, <right>) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case                   | left                 | right                                | result        | value      |
        | arrays that differ     | array(1, 2)          | array(3, 4)                          | array<int>    | [1, 2]     |
        | arrays that match      | array(1, 2)          | array(1, 2)                          | array<int>    | NULL       |
        | structs that match     | named_struct('a', 1) | named_struct('a', 1)                 | struct<a:int> | NULL       |
        | strings                | 'x'                  | 'y'                                  | string        | x          |
        | dates                  | DATE '2024-01-01'    | DATE '2024-01-02'                    | date          | 2024-01-01 |

    @sail-bug
    Scenario Outline: nullif over <case> (known Sail bug)
      When query
        """
        SELECT typeof(nullif(<left>, <right>)) AS t, CAST(nullif(<left>, <right>) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case                   | left                 | right                                | result        | value      |
        | arrays of mixed width  | array(1)             | array(CAST(2 AS BIGINT))             | array<int>    | [1]        |
        | structs of mixed width | named_struct('a', 1) | named_struct('a', CAST(2 AS BIGINT)) | struct<a:int> | {1}        |

    # Binary gets its own scenario because the raw bytes render as replacement characters in a
    # data table; `hex` keeps the assertion readable and exact.
    Scenario: nullif over binary keeps the first argument
      When query
        """
        SELECT typeof(nullif(X'CAFE', X'BEEF')) AS t, hex(nullif(X'CAFE', X'BEEF')) AS v
        """
      Then query result
        | t      | v    |
        | binary | CAFE |

    # Spark rejects these in ANALYSIS (`OrderUtils.isOrderable` via
    # `TypeUtils.checkForOrderingExpr`). Asserting the core of its message rather than the error
    # class keeps the test from breaking on the next Spark minor while still discriminating.
    @sail-bug
    Scenario Outline: nullif over <case> is rejected
      When query
        """
        SELECT nullif(<left>, <right>) AS result
        """
      Then query error does not support ordering on type

      Examples:
        | case                 | left                                 | right                                |
        | maps                 | map('a', 1)                          | map('a', 1)                          |
        | a NULL against a map | NULL                                 | map('a', 1)                          |
        | variants             | parse_json('1')                      | parse_json('1')                      |
        | calendar intervals   | make_interval(1, 2, 3, 4, 5, 6, 7.0) | make_interval(1, 2, 3, 4, 5, 6, 7.0) |
        | an array of maps     | array(map('a', 1))                   | array(map('a', 1))                   |
        | a struct of maps     | named_struct('m', map('a', 1))       | named_struct('m', map('a', 1))       |

    # The other side of the rule: an ANSI interval IS an AtomicType in Spark, so it is orderable
    # and must NOT be rejected. A rule made only of rejections is satisfied by an implementation
    # that rejects everything.
    # Asserted by equality rather than by `typeof`: Sail widens a year interval to
    # `interval year to month`, an unrelated divergence in how Arrow carries interval fields.
    # What this scenario pins is that the call is ACCEPTED and returns its first argument.
    Scenario: nullif over year-month intervals is accepted
      When query
        """
        SELECT nullif(INTERVAL '1' YEAR, INTERVAL '2' YEAR) = INTERVAL '1' YEAR AS accepted
        """
      Then query result
        | accepted |
        | true     |

  # Spark binds `left` once (`With`, `nullExpressions.scala:178`) so it is evaluated a single time
  # even though the expansion mentions it three times. Building this as a plan-time `CASE` instead
  # referenced `left` twice, and a non-deterministic argument was then compared and returned
  # separately: 7 of 30 rows came back as the very value the call had just matched.
  #
  # Measured across `spark.sql.ansi.enabled` x `spark.sql.alwaysInlineCommonExpr`: Spark itself
  # leaks ~470/2000 rows once the inline config is on, because that is the branch that duplicates
  # `left`. It is `.internal()` and documented as an escape hatch for `With` bugs, so Sail does not
  # implement it and the scenario below pins only the default. Asserting the inline cell either way
  # would freeze a bug or fail against the JVM oracle.
  Rule: A non-deterministic first argument is evaluated once

    # `nullif(x, 1)` can never return 1. The assertion holds whatever the draws are, so it does not
    # depend on the two engines producing the same random values.
    Scenario: nullif never returns the value it matched
      When query
        """
        SELECT count(*) AS leaked
        FROM (
          SELECT nullif(CAST(rand() * 3 AS INT), CAST(1 AS BIGINT)) AS r FROM range(500)
        )
        WHERE r = 1
        """
      Then query result
        | leaked |
        | 0      |

  # The string family, which Spark settles BEFORE any numeric coercion
  # (`AnsiStringPromotionTypeCoercion.findWiderTypeForString` under ANSI,
  # `TypeCoercion.findCommonTypeForBinaryComparison` without it). It is the other half of the
  # precision-loss rule the ANSI rule below covers, and DataFusion models neither.
  Rule: A string operand promotes the way Spark promotes it

    # 16777216 and 16777217 are the same FLOAT and different DOUBLEs, so the promotion decides
    # whether the row is nulled.
    # Asserted by equality, not by rendering: Sail prints doubles differently from Java
    # (`1.6777216E7` vs `16777216.0`), which is a separate divergence this scenario must not
    # depend on. `survived` is true only if the row came back, i.e. was not nulled.
    @sail-bug
    Scenario: a string against a float compares as double under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nullif(CAST(16777216 AS FLOAT), '16777217') = CAST(16777216 AS FLOAT) AS survived
        """
      Then query result
        | survived |
        | true     |

    # The non-ANSI half of the same pair: without ANSI the string promotes to the FLOAT itself,
    # in which 16777216 and 16777217 are one number, so the row IS nulled. The two modes disagree
    # on the answer, which is what makes the pair worth pinning on both sides.
    Scenario: a string against a float compares as float when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(nullif(CAST(16777216 AS FLOAT), '16777217') AS STRING) AS v
        """
      Then query result
        | v    |
        | NULL |

    # A decimal with a non-zero scale promotes to DOUBLE in BOTH modes — "there is no proper
    # decimal type we can pick" (SPARK-22469) — so '1.04' does not collapse onto 1.0.
    @sail-bug
    Scenario: a string against a scaled decimal compares as double
      When query
        """
        SELECT CAST(nullif(CAST(1.0 AS DECIMAL(2,1)), '1.04') AS STRING) AS v
        """
      Then query result
        | v   |
        | 1.0 |

    # Any other atomic type wins over the string, so this resolves rather than failing.
    @sail-bug
    Scenario: a string against a boolean compares as boolean
      When query
        """
        SELECT typeof(nullif('true', true)) AS t, CAST(nullif('true', true) AS STRING) AS v
        """
      Then query result
        | t      | v    |
        | string | NULL |

  # Spark picks the whole coercion rule LIST by mode, not just the rules' contents:
  # `BooleanEqualityTypeCoercion` is registered in the non-ANSI list only (`TypeCoercion.scala`),
  # and rewrites `EqualTo(bool, numeric)` by casting the boolean. Sail derives one comparison type
  # per pair and has no equivalent of "this rule does not exist in the other mode", so it rejects
  # the pair in both. Measured against Spark JVM 4.2.0.
  Rule: A boolean against a numeric follows the non-ANSI boolean-equality rule

    @sail-bug
    Scenario Outline: nullif of a boolean against <case> without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(nullif(true, <right>)) AS t, CAST(nullif(true, <right>) AS STRING) AS v
        """
      Then query result
        | t        | v       |
        | <result> | <value> |

      Examples:
        | case               | right | result  | value |
        | a matching 1       | 1     | boolean | NULL  |
        | a non-matching 2   | 2     | boolean | true  |

    # The ANSI half: both engines reject, so this pins the accept/reject decision. The message is
    # not asserted because it is a type rejection the planner makes before execution, where the
    # wording is not part of the contract.
    Scenario: a boolean against a numeric is rejected under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nullif(true, 1) AS result
        """
      Then query error .*

  # `findWiderTypeForTwo` maps a fractional against a decimal to DOUBLE in BOTH modes
  # (`TypeCoercionHelper.scala`: "case (_: FractionalType, _: DecimalType) => Some(DoubleType)").
  # DataFusion deliberately does the opposite — "prefer decimal data type over floating point for
  # comparison operation" — so Sail compares in the decimal and the two operands collapse onto the
  # same value. The declared type is correct either way; only the comparison diverges.
  @sail-bug
  Rule: A decimal against a floating point compares as double

    Scenario: 0.1 + 0.2 does not equal 0.3 once the comparison is a double
      When query
        """
        SELECT typeof(nullif(CAST(0.3 AS DECIMAL(2,1)), 0.1D + 0.2D)) AS t,
               CAST(nullif(CAST(0.3 AS DECIMAL(2,1)), 0.1D + 0.2D) AS STRING) AS v
        """
      Then query result
        | t            | v   |
        | decimal(2,1) | 0.3 |

  # The comparison type is part of the contract, and for one pair Spark makes it ANSI-dependent:
  # an integral against a FLOAT widens to DOUBLE under ANSI "to avoid potential precision loss on
  # converting the Integral type as Float type" (`AnsiTypeCoercion.scala:113-124`), and to FLOAT
  # without it (`TypeCoercion.scala`). 16777217 and 16777216 are equal as FLOAT and different as
  # DOUBLE, so the two modes disagree on the answer, not just on the type.
  Rule: The comparison type follows ANSI mode

    @sail-bug
    Scenario: an integral against a float compares as double under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(nullif(CAST(16777217 AS BIGINT), CAST(16777216 AS FLOAT))) AS t,
               CAST(nullif(CAST(16777217 AS BIGINT), CAST(16777216 AS FLOAT)) AS STRING) AS v
        """
      Then query result
        | t      | v        |
        | bigint | 16777217 |

    @sail-bug
    Scenario: an integral against a float compares as float when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(nullif(CAST(16777217 AS BIGINT), CAST(16777216 AS FLOAT))) AS t,
               CAST(nullif(CAST(16777217 AS BIGINT), CAST(16777216 AS FLOAT)) AS STRING) AS v
        """
      Then query result
        | t      | v    |
        | bigint | NULL |

  Rule: Unorderable arguments

    # The root gap is wider than this function: Spark requires `RowOrdering.isOrderable` for every
    # comparison, and Sail compares the storage type instead, which for GEOMETRY is plain BINARY.
    # Measured on 4.2, Sail also answers `g = g` (true), `g < g` (false), `g IN (g)` (true) and
    # `ORDER BY g` where Spark raises the same class. `nullif(a, b)` is `if (a = b) null else a`,
    # so it falls to the equality it makes of its own arguments. One scenario stands for the root;
    # fixing the comparison guard turns all five green, and `max_by(v, nullif(g, g))` with them.
    @sail-bug @spark-4.2
    Scenario: nullif rejects arguments Spark cannot compare
      When query
        """
        SELECT nullif(st_geomfromwkb(w), st_geomfromwkb(w)) AS result
        FROM VALUES (X'0101000000000000000000F03F0000000000000040') AS t(w)
        """
      Then query error DATATYPE_MISMATCH.INVALID_ORDERING_TYPE
