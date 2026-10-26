Feature: div (integer division)

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to div yields the schema Spark declares
      When query
        """
        SELECT 3 div 2 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  # NOTE: Division-by-zero edge cases (literal and dynamic, both ANSI modes)
  # live in `divide_by_zero.feature`. This file focuses on type coverage,
  # overflow, sign/truncation, NULL propagation, intervals, and multi-row.

  # Spark rejects every arity but 2, via `wrongNumArgsError`
  # (QueryCompilationErrors.scala:1009-1016). `IntegralDivide` exposes a single
  # all-`Expression` constructor, so `expectedNum` is "2" and, with no legacy config,
  # the subclass is WITHOUT_SUGGESTION. `.*` spans the subclass so the pattern survives a
  # subclass change; the trailing count is what discriminates one arity from another.
  # SQLSTATE is deliberately left out: 3.5.9 does not append it and the suite runs as an
  # oracle against that version too.
  Rule: Argument count validation

    Scenario Outline: div with <n> arguments is rejected
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error ^\[WRONG_NUM_ARGS.*`div` requires 2 parameters but the actual number is <n>\.

      Examples:
        | n | args      |
        | 0 |           |
        | 1 | 10        |
        | 3 | 10, 3, 1  |

  Rule: Basic integer division returns BIGINT

    Scenario: div positive integers
      When query
        """
        SELECT div(10, 3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div negative dividend truncates toward zero
      When query
        """
        SELECT div(-10, 3) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: div negative divisor truncates toward zero
      When query
        """
        SELECT div(10, -3) AS result
        """
      Then query result
        | result |
        | -3     |

    Scenario: div both negative
      When query
        """
        SELECT div(-10, -3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div zero dividend
      When query
        """
        SELECT div(0, 5) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: div exact division
      When query
        """
        SELECT div(9, 3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div result is BIGINT regardless of input
      When query
        """
        SELECT div(CAST(6 AS TINYINT), CAST(2 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: NULL propagation on non-zero divisor

    Scenario: div NULL dividend returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INT), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div NULL divisor returns NULL
      When query
        """
        SELECT div(10, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div both NULL returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    # An untyped NULL is not one of the four types `IntegralDivide` accepts, but Spark
    # rewrites `NullType` to the expected concrete type before the check, so it resolves
    # in either position and in both ANSI modes. This is what keeps a type allow-list
    # from rejecting it.
    Scenario Outline: div with an untyped NULL <position> returns NULL under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | position   | ansi  | args    |
        | dividend   | true  | NULL, 5 |
        | dividend   | false | NULL, 5 |
        | divisor    | true  | 5, NULL |
        | divisor    | false | 5, NULL |
        | both sides | true  | NULL, NULL |
        | both sides | false | NULL, NULL |

    # The complement of the untyped-NULL rows above: a NULL of a REJECTED type is still
    # rejected, because the acceptability check keys off the declared type, never the
    # value. STRING is the discriminating row — the same NULL value is an error outside
    # ANSI and a result under it, which pins that the promotion runs on the type before
    # the value is ever looked at.
    Scenario Outline: div with a NULL <type> dividend is rejected under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(CAST(NULL AS <type>), 5) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | type    | ansi  |
        | DOUBLE  | true  |
        | DOUBLE  | false |
        | BOOLEAN | true  |
        | BOOLEAN | false |
        | DATE    | true  |
        | DATE    | false |
        | STRING  | false |

    Scenario: div with a NULL STRING dividend returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(NULL AS STRING), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    # An untyped NULL pairs with ANY accepted family, not just the numeric ones: Spark
    # rewrites it to the concrete type the other operand carries, so the division resolves
    # and propagates NULL instead of failing to coerce.
    Scenario Outline: div with an untyped NULL beside <family> returns NULL under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | family        | ansi  | args                        |
        | INTERVAL DAY  | true  | NULL, INTERVAL '2' DAY      |
        | INTERVAL DAY  | false | INTERVAL '10' DAY, NULL     |
        | INTERVAL YEAR | true  | NULL, INTERVAL '2' YEAR     |
        | INTERVAL YEAR | false | INTERVAL '10' YEAR, NULL    |
        | DECIMAL       | true  | NULL, CAST(2 AS DECIMAL(5,2)) |
        | DECIMAL       | false | CAST(5 AS DECIMAL(5,2)), NULL |

    # Mixed families are rejected as a PAIR, the way Spark rejects any operand pair whose
    # types differ, rather than slipping past a per-operand check into DataFusion.
    Scenario Outline: div rejects the mixed pair <case>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | case             | args                                        |
        | BIGINT / IDAY    | CAST(10 AS BIGINT), INTERVAL '2' DAY        |
        | IDAY / BIGINT    | INTERVAL '10' DAY, CAST(2 AS BIGINT)        |
        | IYEAR / IDAY     | INTERVAL '10' YEAR, INTERVAL '2' DAY        |
        | DECIMAL / IDAY   | CAST(5 AS DECIMAL(5,2)), INTERVAL '2' DAY   |
        | IYEAR / INT      | INTERVAL '10' YEAR, 2                       |
        | INT / IYEAR      | 2, INTERVAL '10' YEAR                       |
        | IYEAR / DECIMAL  | INTERVAL '10' YEAR, CAST(2 AS DECIMAL(5,2)) |

  Rule: Overflow semantics — LONG_MIN div -1
    # Two's-complement: BIGINT range is [-2^63, 2^63-1]; -LONG_MIN overflows
    # BIGINT, so under ANSI=false Spark wraps to LONG_MIN (Java `/`); ANSI=true raises
    # ARITHMETIC_OVERFLOW.

    Scenario: div LONG_MIN by -1 wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |

    Scenario: div INT MIN by -1 widens to BIGINT (no overflow)
      When query
        """
        SELECT div(-2147483648, -1) AS result
        """
      Then query result
        | result     |
        | 2147483648 |

  # From #2822. Same arithmetic as the scenario above, reached through the INFIX spelling and
  # paired over the mode; kept because neither of those axes was covered here.
  Rule: Integral division widens operands before computing the quotient

    Scenario Outline: INT minimum divided by negative one fits BIGINT with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -2147483648 DIV -1 AS result
        """
      Then query result
        | result     |
        | 2147483648 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: div LONG_MAX by 1 is identity
      When query
        """
        SELECT div(CAST(9223372036854775807 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result              |
        | 9223372036854775807 |

    Scenario: div LONG_MAX by -1 negates without overflow
      When query
        """
        SELECT div(CAST(9223372036854775807 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result               |
        | -9223372036854775807 |

    Scenario: div zero by zero returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(0 AS BIGINT), CAST(0 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div zero by zero errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(0 AS BIGINT), CAST(0 AS BIGINT)) AS result
        """
      Then query error Division by zero

    Scenario: div BIGINT column containing LONG_MIN with -1 wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)),
          (CAST(-10 AS BIGINT), CAST(2 AS BIGINT))
        AS t(a, b)
        """
      Then query result
        | result                |
        | 3                     |
        | -9223372036854775808  |
        | -5                    |

    Scenario: div BIGINT column containing LONG_MIN with -1 errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT))
        AS t(a, b)
        """
      Then query error ^\[ARITHMETIC_OVERFLOW\] Overflow in integral divide

  Rule: Mixed numeric types

    Scenario: div TINYINT by BIGINT widens to BIGINT
      When query
        """
        SELECT div(CAST(10 AS TINYINT), CAST(3 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div BIGINT by TINYINT widens to BIGINT
      When query
        """
        SELECT div(CAST(10 AS BIGINT), CAST(3 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div INT by DECIMAL
      When query
        """
        SELECT div(10, CAST(3.0 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div DECIMAL by INT truncates toward zero
      When query
        """
        SELECT div(CAST(3.0 AS DECIMAL(5,2)), 10) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: div DECIMAL by DECIMAL with different precision
      When query
        """
        SELECT div(CAST(10 AS DECIMAL(10,0)), CAST(3 AS DECIMAL(20,0))) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: DECIMAL division

    Scenario: div DECIMAL inputs returns BIGINT
      When query
        """
        SELECT div(CAST(7.5 AS DECIMAL(5,2)), CAST(2.5 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    # `divideToIntegralValue` truncates toward zero, so the sign of the quotient is the product
    # of the operands' signs and the magnitude never rounds away from zero: 10.9 / -3.0 is
    # -3.63, not -4. One scenario covers one of the four sign arms, so all four are here.
    Scenario Outline: div DECIMAL truncates toward zero with <case>
      When query
        """
        SELECT div(CAST(<dividend> AS DECIMAL(5,2)), CAST(<divisor> AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | dividend | divisor | result |
        | a positive pair             | 10.9     | 3.0     | 3      |
        | a negative dividend         | -10.9    | 3.0     | -3     |
        | a negative divisor          | 10.9     | -3.0    | -3     |
        | a negative pair             | -10.9    | -3.0    | 3      |

    # The sign arms above are folded literals; this one reaches the kernel from a COLUMN.
    Scenario: div DECIMAL columns truncate toward zero with a negative divisor
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES (CAST(10.9 AS DECIMAL(5,2)), CAST(-3.0 AS DECIMAL(5,2))) AS t(a, b)
        """
      Then query result
        | result |
        | -3     |


  Rule: INTERVAL division

    Scenario: div two INTERVAL DAY
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '2' DAY) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div two INTERVAL YEAR
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '2' YEAR) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div INTERVAL DAY by zero INTERVAL DAY returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '0' DAY) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div INTERVAL DAY multi-row with zero divisor returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result FROM VALUES
          (INTERVAL '10' DAY, INTERVAL '2' DAY),
          (INTERVAL '5' DAY, INTERVAL '0' DAY)
        AS t(a, b)
        """
      Then query result
        | result |
        | 5      |
        | NULL   |

    Scenario: div INTERVAL YEAR by zero INTERVAL YEAR returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '0' YEAR) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div INTERVAL DAY multi-row with zero divisor errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES
          (INTERVAL '10' DAY, INTERVAL '2' DAY),
          (INTERVAL '5' DAY, INTERVAL '0' DAY)
        AS t(a, b)
        """
      Then query error Division by zero

    Scenario: div NULL INTERVAL returns NULL
      When query
        """
        SELECT div(CAST(NULL AS INTERVAL DAY), INTERVAL '2' DAY) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Type rejection
    # Spark's IntegralDivide rejects FLOAT and DOUBLE at analysis time
    # (BINARY_OP_WRONG_TYPE / BINARY_OP_DIFF_TYPES). Sail mirrors that in
    # the `spark_div` dispatcher — see workarounds below for valid patterns.

    Scenario: div DATE errors
      When query
        """
        SELECT div(DATE '2024-01-15', DATE '2024-01-01') AS result
        """
      Then query error due to data type mismatch

    Scenario: div INTERVAL DAY by INT errors
      When query
        """
        SELECT div(INTERVAL '10' DAY, 2) AS result
        """
      Then query error due to data type mismatch

    Scenario: div FLOAT/FLOAT errors
      When query
        """
        SELECT div(CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT)) AS result
        """
      Then query error due to data type mismatch

    Scenario: div DOUBLE/DOUBLE errors
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query error due to data type mismatch

    Scenario: div INT/DOUBLE errors
      When query
        """
        SELECT div(10, CAST(3.0 AS DOUBLE)) AS result
        """
      Then query error due to data type mismatch

  Rule: Workarounds for FLOAT and DOUBLE operands
    # Spark rejects FLOAT/DOUBLE in div by design (IntegralDivide requires
    # integral or decimal types). Users can still perform integer division
    # on floating-point values by casting first. These scenarios document
    # the valid workarounds.

    Scenario: div accepts DOUBLE values cast to DECIMAL
      When query
        """
        SELECT div(CAST(CAST(1.5 AS DOUBLE) AS DECIMAL(10,2)),
                   CAST(CAST(0.3 AS DOUBLE) AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: div accepts DOUBLE values cast to BIGINT
      When query
        """
        SELECT div(CAST(CAST(10.7 AS DOUBLE) AS BIGINT),
                   CAST(CAST(3.2 AS DOUBLE) AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: Multi-row vectorized path

    Scenario: div BIGINT column with mixed signs and NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES
          (CAST(10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(-10 AS BIGINT), CAST(3 AS BIGINT)),
          (CAST(10 AS BIGINT), CAST(0 AS BIGINT)),
          (CAST(NULL AS BIGINT), CAST(5 AS BIGINT)),
          (CAST(5 AS BIGINT), CAST(NULL AS BIGINT))
        AS t(a, b)
        """
      Then query result
        | result |
        | 3      |
        | -3     |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: div INT column exact division
      When query
        """
        SELECT div(a, b) AS result
        FROM VALUES (15, 3), (20, 5), (7, 2), (100, 10) AS t(a, b)
        """
      Then query result
        | result |
        | 5      |
        | 4      |
        | 3      |
        | 10     |

  # Outside ANSI the legacy promotion sends a STRING to DOUBLE, which `div` rejects, so every
  # combination with a string fails. Under ANSI it resolves only when exactly one side is a
  # string and the other is integral. Both halves are pinned so neither direction is vacuous.
  Rule: STRING operands follow ANSI-dependent promotion

    Scenario Outline: div <case> is rejected under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | case                 | args                                    |
        | INT by STRING        | 5, '2'                                  |
        | STRING by INT        | '5', 2                                  |
        | BIGINT by STRING     | CAST(5 AS BIGINT), '2'                  |
        | STRING by STRING     | '10', '3'                               |
        | DECIMAL by STRING    | CAST(5 AS DECIMAL(5,2)), '2'            |
        | STRING by DECIMAL    | '5', CAST(2 AS DECIMAL(5,2))            |
        | INTERVAL by STRING   | INTERVAL '10' DAY, '2'                  |
        | STRING by INTERVAL   | '2', INTERVAL '10' DAY                  |
        | INT by unparseable   | 5, 'abc'                                |
        | INT by STRING zero   | 5, '0'                                  |
        | INT by NULL STRING   | 5, CAST(NULL AS STRING)                 |

    Scenario Outline: div <case> is rejected under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | case               | args                         |
        | STRING by STRING   | '10', '3'                    |
        | DECIMAL by STRING  | CAST(5 AS DECIMAL(5,2)), '2' |
        | STRING by DECIMAL  | '5', CAST(2 AS DECIMAL(5,2)) |
        | INTERVAL by STRING | INTERVAL '10' DAY, '2'       |
        | STRING by INTERVAL | '2', INTERVAL '10' DAY       |

    Scenario Outline: div <case> is accepted under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | args                    | result |
        | INT by STRING      | 5, '2'                  | 2      |
        | STRING by INT      | '5', 2                  | 2      |
        | BIGINT by STRING   | CAST(5 AS BIGINT), '2'  | 2      |
        | INT by NULL STRING | 5, CAST(NULL AS STRING) | NULL   |

    # Once the string is widened to LONG the rest falls out of the ordinary rules: an
    # unparseable string fails the cast, and '0' divides by zero.
    # TODO: not a `div` bug — the message comes from the `cast(_, BIGINT)` the plan builder
    # inserts for the ANSI string promotion, so it is DataFusion's wording ("Cannot cast string
    # 'abc' to value of Int64 type") where Spark raises CAST_INVALID_INPUT. The exception class
    # already matches (NumberFormatException, which is what Spark uses for a numeric target) and
    # no value changes; only the text diverges.
    # Fixable engine-wide, not here: `cast_error_to_throwable` (sail-spark-connect/src/error.rs)
    # already parses Arrow's cast message to pick the throwable, so it is the one place that can
    # rewrite the text too. That closes every ANSI cast-message divergence at once instead of
    # making `div` the only site in the engine that emits the Spark class — measured blast
    # radius: 36 cast assertions across 21 feature files, of which exactly one depends on
    # DataFusion's literal wording.
    @sail-bug
    Scenario: div INT by an unparseable STRING errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(5, 'abc') AS result
        """
      Then query error ^\[CAST_INVALID_INPUT\]

    Scenario: div INT by STRING zero errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(5, '0') AS result
        """
      Then query error Division by zero

  # Every other STRING scenario above uses a literal, so all of them are folded before the plan
  # builder sees them. These reach the promotion from a COLUMN, which is the shape that tells a
  # type-driven rewrite from a constant-folding accident. Values measured on the Spark 4.2.0 JVM.
  # Note what these do NOT cover: SQL always resolves a string to `Utf8` (measured on all three
  # of a VALUES column, a literal and a CAST), so the `Utf8View` arm of the rejection's
  # `sameType` is unreachable from SQL and stays untested here.
  Rule: STRING columns follow the same promotion as STRING literals

    Scenario Outline: div <case> from columns is accepted under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (<values>) AS t(a, b)
        """
      Then query result
        | result |
        | 3      |

      Examples:
        | case             | values                  |
        | STRING by INT    | '10', 3                 |
        | STRING by BIGINT | '10', CAST(3 AS BIGINT) |

    Scenario Outline: div <case> from columns is rejected with BINARY_OP_WRONG_TYPE, ansi <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (<values>) AS t(a, b)
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_WRONG_TYPE\] Cannot resolve ".* div .*" due to data type mismatch: .*not "<type>"\.

      Examples:
        | case             | values    | type   | ansi  |
        | STRING by STRING | '10', '3' | STRING | true  |
        | STRING by STRING | '10', '3' | DOUBLE | false |

    Scenario Outline: div <case> from columns is rejected with BINARY_OP_DIFF_TYPES without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (<values>) AS t(a, b)
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve ".* div .*" due to data type mismatch: .*incompatible types \("DOUBLE" and "BIGINT"\)\.

      Examples:
        | case             | values                  |
        | STRING by INT    | '10', 3                 |
        | STRING by BIGINT | '10', CAST(3 AS BIGINT) |

  # Everything below is a difference from `main` that nothing pinned. Each value was measured on
  # the Spark 4.2.0 JVM and, separately, on `main`, so the comment states which engine each
  # scenario is defending against.
  Rule: div evaluates lazily and promotes strings the way Spark does

    # Spark's `ConstantFolding` never evaluates the untaken branch, so an ANSI zero divisor
    # inside it raises nothing (`DivModLike.eval`, arithmetic.scala:664-684, is simply not
    # reached). `main` raises here because the old plan builder short-circuited a LITERAL zero
    # divisor at plan time, before any branch pruning could happen; removing that short-circuit
    # is what makes these three agree with Spark. Measured: Spark 0, main raises, branch 0.
    Scenario Outline: div by a literal zero in an untaken <form> is never evaluated under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result |
        | 0      |

      Examples:
        | form      | expression                                     |
        | IF        | IF(false, div(1, 0), 0)                        |
        | CASE      | CASE WHEN false THEN div(1, 0) ELSE 0 END      |

    # The same, with the predicate in a COLUMN so no folding can prune the branch at plan time.
    Scenario: div by a literal zero under a column predicate is never evaluated under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT IF(b, div(1, 0), 0) AS result FROM VALUES (false) AS t(b)
        """
      Then query result
        | result |
        | 0      |

    # Spark's rule is FOLDABLE, not literal: `1-1` folds to zero and takes the same zero-divisor
    # path as the bare `0`. The removed plan-time gate keyed on `Expr::Literal` only, so this
    # shape proves the zero policy now lives where Spark puts it. Measured: `main` answers the
    # ANSI case with an AnalysisException carrying the non-Spark text "Division by zero".
    Scenario: div by a foldable non-literal zero returns NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(5, 1-1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div by a foldable non-literal zero raises under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(5, 1-1) AS result
        """
      Then query error of class ArithmeticException matching ^\[DIVIDE_BY_ZERO\]

    # ANSI promotes a STRING beside an integral to LONG (`findWiderTypeForString`,
    # AnsiStringPromotionTypeCoercion.scala:93), and Spark's STRING->LONG cast TRIMS surrounding
    # whitespace (`UTF8StringUtils.toLongExact`). `main` cannot express this at all: it refuses
    # the pair with "Cannot coerce arithmetic expression Utf8 / Int32". Measured: Spark 5 for all
    # three paddings.
    Scenario Outline: div promotes a <case> STRING dividend under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(<dividend>, 2) AS result
        """
      Then query result
        | result |
        | 5      |

      Examples:
        | case              | dividend |
        | both-side padded  | ' 10 '   |
        | trailing-padded   | '10 '    |
        | leading-padded    | ' 10'    |

  # `div` always declares BIGINT, including when the divisor is a literal zero that
  # folds to NULL under ANSI=false. The value is NULL under either rule, so only the
  # schema discriminates; the non-zero and column-fed scenarios are the controls that
  # keep this rule from being satisfied by an implementation that types everything as
  # VOID.
  @function(nullability)
  Rule: Literal zero divisor keeps the declared BIGINT output type

    Scenario: div by literal zero declares BIGINT under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 5 div 0 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: div by non-zero literal declares BIGINT under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 5 div 2 AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: div by zero from columns declares BIGINT under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a div b AS result FROM VALUES (5, 0) AS t(a, b)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  # A zero divisor must not shadow the type check: Spark rejects a FLOAT/DOUBLE or
  # otherwise unsupported operand during analysis whatever the divisor's value is.
  # These pair with the non-zero-divisor rejections above; the pair is what makes the
  # rule discriminating, since a check that ran only for non-zero divisors passes those.
  Rule: Type rejection is not shadowed by a zero divisor

    Scenario Outline: div <case> is rejected under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error due to data type mismatch

      Examples:
        | case                    | ansi  | args                                     |
        | DOUBLE by literal zero  | false | CAST(1.0 AS DOUBLE), 0                   |
        | DOUBLE by literal zero  | true  | CAST(1.0 AS DOUBLE), 0                   |
        | DOUBLE by zero DOUBLE   | false | CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE) |
        | DOUBLE by zero DOUBLE   | true  | CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE) |
        | BOOLEAN by literal zero | false | true, 0                                  |
        | BOOLEAN by literal zero | true  | true, 0                                  |
        | DATE by literal zero    | false | DATE '2024-01-15', 0                     |
        | DATE by literal zero    | true  | DATE '2024-01-15', 0                     |

  # Spark evaluates the divisor first, but a NULL dividend still wins over a zero
  # divisor: the result is NULL, not an error, even under ANSI. The non-NULL dividend
  # scenarios are the other half — without them a guard that never raised would pass.
  Rule: A NULL dividend wins over a zero divisor under ANSI

    Scenario: div NULL BIGINT dividend by literal zero returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(NULL AS BIGINT), 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div NULL BIGINT dividend by zero column returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (CAST(NULL AS BIGINT), CAST(0 AS BIGINT)) AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div NULL INTERVAL DAY dividend by zero interval returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (CAST(NULL AS INTERVAL DAY), INTERVAL '0' DAY) AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div NULL INTERVAL YEAR dividend by zero interval returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (CAST(NULL AS INTERVAL YEAR), INTERVAL '0' YEAR) AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div non-NULL dividend by zero column still errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (CAST(5 AS BIGINT), CAST(0 AS BIGINT)) AS t(a, b)
        """
      Then query error Division by zero

  # Spark's IntegralDivide accepts only the two ANSI interval families; CalendarInterval
  # (Sail's MonthDayNano) is rejected during analysis rather than divided with an
  # invented 30-day month, which also removes the i64 overflow that normalization hit.
  Rule: CalendarInterval operands are rejected

    Scenario: div two calendar intervals is rejected
      When query
        """
        SELECT div(make_interval(1, 0, 0, 2, 0, 0, 0), make_interval(1, 0, 0, 1, 0, 0, 0)) AS result
        """
      Then query error due to data type mismatch

    Scenario: div wide calendar interval does not overflow into a wrong answer
      When query
        """
        SELECT div(make_interval(0, 4000, 0, 1, 0, 0, 0), make_interval(0, 1, 0, 1, 0, 0, 0)) AS result
        """
      Then query error due to data type mismatch

  # Spark builds the default column name from `sqlOperator` through `BinaryOperator.sql`
  # (Expression.scala:859), so an unaliased `div` is named `(7 div 2)` — the infix form, even
  # when written as `div(7, 2)`. Every other scenario in this file aliases with `AS result`,
  # so nothing else covers the generated name.
  Rule: div names an unaliased column the way Spark does

    # `sqlOperator` is a constant, so the name does not depend on how the call was spelled.
    # Measured on the JVM: all three forms give `(7 div 2)`.
    Scenario Outline: an unaliased div column carries Spark's operator name: <case>
      When query
        """
        SELECT <expr>
        """
      Then query result
        | (7 div 2) |
        | 3         |

      Examples:
        | case            | expr       |
        | infix            | 7 div 2    |
        | call form       | div(7, 2)  |
        | uppercase call  | DIV(7, 2)  |

  # `IntegralDivide` declares BIGINT and `nullable = true` unconditionally, for every
  # operand family and in both ANSI modes, because `DivModLike` hardcodes the flag
  # rather than deriving it from the operands. In Sail each family reaches the output
  # type by a different mechanism, so every family is asserted separately: the value is
  # identical either way and only the schema discriminates.
  @function(nullability)
  Rule: div declares a nullable BIGINT for every operand family

    Scenario Outline: div over <family> declares nullable BIGINT under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

      Examples:
        | family        | ansi  | args                                                |
        | BIGINT        | true  | CAST(7 AS BIGINT), CAST(2 AS BIGINT)                |
        | BIGINT        | false | CAST(7 AS BIGINT), CAST(2 AS BIGINT)                |
        | DECIMAL       | true  | CAST(7 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(5,2))    |
        | DECIMAL       | false | CAST(7 AS DECIMAL(5,2)), CAST(2 AS DECIMAL(5,2))    |
        | INTERVAL DAY  | true  | INTERVAL '10' DAY, INTERVAL '2' DAY                 |
        | INTERVAL DAY  | false | INTERVAL '10' DAY, INTERVAL '2' DAY                 |
        | INTERVAL YEAR | true  | INTERVAL '10' YEAR, INTERVAL '2' YEAR               |
        | INTERVAL YEAR | false | INTERVAL '10' YEAR, INTERVAL '2' YEAR               |
        | NULL / INT    | true  | NULL, 5                                             |
        | NULL / NULL   | false | NULL, NULL                                          |
        | NULL / IDAY   | true  | NULL, INTERVAL '2' DAY                              |
        | NULL / DECIMAL| false | NULL, CAST(2 AS DECIMAL(5,2))                       |
        | INT / STRING  | true  | 5, '2'                                              |

  # DECIMAL reaches `SparkIntegerDiv`'s decimal kernel, which owns both zero policies
  # (ANSI raises, otherwise NULL) exactly like the interval kernel, so no planner guard is
  # wrapped around the divisor. The NULL-dividend scenario pins that a NULL dividend still
  # wins over a zero divisor under ANSI, which the kernel gets from its `(Some(a), Some(b))` arm.
  Rule: DECIMAL zero divisor follows ANSI mode

    Scenario: div DECIMAL by zero DECIMAL returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, CAST(0.0 AS DECIMAL(5,2))) AS result
        FROM VALUES (CAST(5.5 AS DECIMAL(5,2))) AS t(a)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: div DECIMAL by zero DECIMAL errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, CAST(0.0 AS DECIMAL(5,2))) AS result
        FROM VALUES (CAST(5.5 AS DECIMAL(5,2))) AS t(a)
        """
      Then query error Division by zero

    Scenario: div NULL DECIMAL dividend by zero DECIMAL returns NULL under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(a, CAST(0.0 AS DECIMAL(5,2))) AS result
        FROM VALUES (CAST(NULL AS DECIMAL(5,2))) AS t(a)
        """
      Then query result
        | result |
        | NULL   |

  # Both boundaries are implemented by this change and assert parity, so they are untagged.
  # They sit apart because each was found by the full type matrix rather than by reading the
  # expression, and each needs a value at the very edge of its type to move at all.
  Rule: Numeric boundaries at the type edges

    # Spark selects `PhysicalIntegerType.integral` for YEAR TO MONTH (arithmetic.scala:916-917),
    # so it divides in 32-bit and wraps on `Int.MinValue / -1`, then widens with
    # `integral.toLong`. Widening to i64 first would return the unwrapped positive value, which
    # is why the kernel divides in i32. `checkDivideOverflow` is LONG-dividend-only
    # (arithmetic.scala:885-888), so Spark never raises here and no ANSI pairing is needed.
    Scenario: div YEAR-MONTH interval wraps in 32-bit at the Int boundary
      When query
        """
        SELECT div(make_ym_interval(-178956970, -8), INTERVAL '-1' MONTH) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    # `IntegralDivide` OVERRIDES `resultDecimalType` (arithmetic.scala:899-906) to scale 0 —
    # `DecimalType.bounded(p1 - s1 + s2, 0)` — because the quotient is integral, and it divides
    # WITHOUT bringing the operands to a common type. Routing this through a regular decimal
    # division instead asks for a precision the type system cannot express once the divisor
    # scale is extreme (`Decimal(20,0) / Decimal(38,37)` wants precision 96), so the type
    # resolution fails before any value is computed. Found by the full 32x32 type matrix;
    # ANSI-invariant, which is why these rows are not paired over the mode.
    Scenario Outline: div by a high-scale DECIMAL divisor: <case>
      When query
        """
        SELECT div(<dividend>, CAST(0.1 AS DECIMAL(38,37))) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | dividend                  | result |
        | INT dividend          | 1                         | 10     |
        | BIGINT dividend       | CAST(1 AS BIGINT)         | 10     |
        | DECIMAL dividend      | CAST(1 AS DECIMAL(10,2))  | 10     |
        | high-scale both sides | CAST(0.1 AS DECIMAL(38,37)) | 1    |
        | NULL BIGINT dividend  | CAST(NULL AS BIGINT)      | NULL   |

    # The quotient is checked against `resultDecimalType` = `bounded(p1 - s1 + s2, 0)`
    # (arithmetic.scala:899-906), NOT against BIGINT: 1e37 / 0.1 is 1e38, which needs 39 digits
    # and does not fit Decimal(38, 0). Measured on the JVM: ANSI raises
    # NUMERIC_VALUE_OUT_OF_RANGE (a third error class for `div`, distinct from DIVIDE_BY_ZERO and
    # ARITHMETIC_OVERFLOW), ANSI off returns NULL. The substring stops before the value so the
    # pattern does not depend on the double space Spark leaves after the empty parent message.
  Rule: div checks the DECIMAL quotient against its result precision

    Scenario: div DECIMAL quotient out of result precision errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(
          CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0)),
          CAST(0.1 AS DECIMAL(38,37))
        ) AS result
        """
      Then query error of class ArithmeticException matching 100000000000000000000000000000000000000 cannot be represented as Decimal\(38, 0\)

    Scenario: div DECIMAL quotient out of result precision returns NULL under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(
          CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0)),
          CAST(0.1 AS DECIMAL(38,37))
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    # A regular decimal division PRE-SCALES the dividend (`l.mul_checked(10^mul_pow)`), which
    # overflows i128 whenever the unscaled dividend is large enough — even when the true
    # quotient is small. Spark never pre-scales: `divideToIntegralValue` runs on unbounded
    # BigDecimal and only then narrows with `Decimal.toLong` (arithmetic.scala:910-933). This
    # pins the quotient that the pre-scaling used to lose; it fits in BIGINT comfortably.
    Scenario: div DECIMAL divides a wide dividend without pre-scaling it
      When query
        """
        SELECT div(
          CAST(100000000000000000000000000000000000 AS DECIMAL(38,0)),
          CAST(100000000000000000000 AS DECIMAL(38,0))
        ) AS result
        """
      Then query result
        | result           |
        | 1000000000000000 |

    # `checkDivideOverflow` (arithmetic.scala:885-888) is enabled only when the dividend is
    # LONG, never for an interval, so Spark wraps here instead of raising — in BOTH ANSI modes,
    # since the gate never reaches its `failOnError` guard for this operand type. Both modes stay
    # pinned on purpose: a regression that only re-checked the ANSI half would otherwise pass.
    Scenario Outline: div DAY-TIME interval wraps instead of overflowing under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(
        INTERVAL '-106751991' DAY - INTERVAL '4' HOUR - INTERVAL '54.775808' SECOND,
        INTERVAL '-0.000001' SECOND
        ) AS result
        """
      Then query result
        | result               |
        | -9223372036854775808 |

      Examples:
        | ansi  |
        | true  |
        | false |

    # Spark divides on BigDecimal and only then narrows with `Decimal.toLong`, i.e.
    # `BigDecimal.longValue()`, which keeps the low 64 bits and therefore WRAPS rather than
    # failing. Paired over the mode because the wrap is ANSI-invariant: the quotient fits
    # `resultDecimalType`, so `checkDecimalOverflow` never fires and neither mode can raise.
    Scenario Outline: div wide DECIMAL narrows to BIGINT by wrapping under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(CAST(12345678901234567890.12345678 AS DECIMAL(38,8)), CAST(1 AS DECIMAL(38,8))) AS result
        """
      Then query result
        | result               |
        | -6101065172474983726 |

      Examples:
        | ansi  |
        | true  |
        | false |

  # Anchored at the start on purpose: a bare substring also matches a wrapper-prefixed
    # message, so only the anchor discriminates. The pattern must stay on the step line — a
    # backslash in an Examples cell arrives at `re.search` doubled (measured); only the bare
    # type names travel in cells.
    # `BinaryOperator.checkInputDataTypes` (Expression.scala:842-856) picks between TWO subclasses
    # and `!sameType` is tested FIRST, so a mismatched pair is always DIFF_TYPES even when neither
    # type is accepted. It runs AFTER the coercion rules rewrote the children, so BOTH the subclass
    # and the names are the POST-coercion ones — the types the user wrote may appear nowhere in the
    # message. Three rules decide what is left behind:
    #   * `IntegralDivisionTypeCoercion.scala:34-37` widens every narrower integral to LONG,
    #     whatever the peer is — so INT is reported as BIGINT even beside a DATE;
    #   * the string rules move the string side: under ANSI `findWiderTypeForString`
    #     (AnsiStringPromotionTypeCoercion.scala:92-109) sends BOTH sides to one type, or neither
    #     moves; otherwise `StringPromotionTypeCoercion.scala:48-54` sends the string alone to
    #     DOUBLE unless the peer is an interval;
    #   * `DecimalPrecisionTypeCoercion.scala:182-185` sends a DECIMAL beside a float to DOUBLE and
    #     leaves the float side alone — so DECIMAL by FLOAT is reported as ("DOUBLE" and "FLOAT").
    # Every row below was measured on the Spark 4.2.0 JVM.
  Rule: div reports the DATATYPE_MISMATCH subclass and the type names Spark picks

    Scenario Outline: div rejects <case> with BINARY_OP_DIFF_TYPES, ansi <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve ".* div .*" due to data type mismatch: .*incompatible types \("<left>" and "<right>"\)\.

      Examples:
        | case                 | args                                            | left         | right        | ansi  |
        | DOUBLE by BIGINT     | CAST(1.0 AS DOUBLE), CAST(2 AS BIGINT)          | DOUBLE       | BIGINT       | true  |
        | DOUBLE by BIGINT     | CAST(1.0 AS DOUBLE), CAST(2 AS BIGINT)          | DOUBLE       | BIGINT       | false |
        | DECIMAL by FLOAT     | CAST(1 AS DECIMAL(5,2)), CAST(2.0 AS FLOAT)     | DOUBLE       | FLOAT        | true  |
        | DECIMAL by FLOAT     | CAST(1 AS DECIMAL(5,2)), CAST(2.0 AS FLOAT)     | DOUBLE       | FLOAT        | false |
        | FLOAT by DECIMAL     | CAST(1.0 AS FLOAT), CAST(2 AS DECIMAL(5,2))     | FLOAT        | DOUBLE       | true  |
        | FLOAT by DECIMAL     | CAST(1.0 AS FLOAT), CAST(2 AS DECIMAL(5,2))     | FLOAT        | DOUBLE       | false |
        | SMALLINT by DATE     | CAST(1 AS SMALLINT), DATE '2024-01-15'          | BIGINT       | DATE         | true  |
        | SMALLINT by DATE     | CAST(1 AS SMALLINT), DATE '2024-01-15'          | BIGINT       | DATE         | false |
        | NULL by DOUBLE       | NULL, CAST(2.0 AS DOUBLE)                       | VOID         | DOUBLE       | true  |
        | NULL by DOUBLE       | NULL, CAST(2.0 AS DOUBLE)                       | VOID         | DOUBLE       | false |
        | NULL by BOOLEAN      | NULL, true                                      | VOID         | BOOLEAN      | true  |
        | NULL by BOOLEAN      | NULL, true                                      | VOID         | BOOLEAN      | false |
        | NULL by DATE         | NULL, DATE '2024-01-15'                         | VOID         | DATE         | true  |
        | NULL by DATE         | NULL, DATE '2024-01-15'                         | VOID         | DATE         | false |
        | DOUBLE by NULL       | CAST(1.0 AS DOUBLE), NULL                       | DOUBLE       | VOID         | true  |
        | DOUBLE by NULL       | CAST(1.0 AS DOUBLE), NULL                       | DOUBLE       | VOID         | false |
        | INT by STRING        | 5, '3'                                          | BIGINT       | DOUBLE       | false |
        | STRING by DATE       | '5', DATE '2024-01-15'                          | DOUBLE       | DATE         | false |
        | STRING by BOOLEAN    | '5', true                                       | DOUBLE       | BOOLEAN      | false |
        | STRING by NULL       | '5', NULL                                       | DOUBLE       | VOID         | false |
        | NULL by STRING       | NULL, '3'                                       | VOID         | DOUBLE       | false |
        | STRING by BINARY     | '5', X'01'                                      | DOUBLE       | BINARY       | false |

    # Spark keeps an interval's field bounds in the TYPE, so `INTERVAL '10' DAY` is
    # `DayTimeIntervalType(DAY, DAY)` and prints as "INTERVAL DAY", and `INTERVAL '1' YEAR` is
    # `YearMonthIntervalType(YEAR, YEAR)` and prints as "INTERVAL YEAR". Sail stores day-time as
    # `Duration(Microsecond)` and year-month as `Interval(YearMonth)` with nowhere to keep the
    # bounds, so every message that names one widens it to "INTERVAL DAY TO SECOND" or
    # "INTERVAL YEAR TO MONTH". BOTH families are affected — measured on the 3.5.9 and 4.2.0
    # JVMs, which agree. The subclass and the peer's name are right; only the interval's own
    # name is wider. Pre-existing and systemic — the fix belongs with the interval type fields.
    @sail-bug
    Scenario Outline: div rejects <case> naming the interval field bounds, ansi <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve ".* div .*" due to data type mismatch: .*incompatible types \("<left>" and "<right>"\)\.

      Examples:
        | case                | args                  | left          | right        | ansi  |
        | INTERVAL DAY by INT  | INTERVAL '10' DAY, 2 | INTERVAL DAY  | BIGINT       | true  |
        | INTERVAL DAY by INT  | INTERVAL '10' DAY, 2 | INTERVAL DAY  | BIGINT       | false |
        | INTERVAL YEAR by INT | INTERVAL '1' YEAR, 2 | INTERVAL YEAR | BIGINT       | true  |
        | INTERVAL YEAR by INT | INTERVAL '1' YEAR, 2 | INTERVAL YEAR | BIGINT       | false |
        | STRING by INTERVAL  | '5', INTERVAL '2' DAY | STRING        | INTERVAL DAY | true  |
        | STRING by INTERVAL  | '5', INTERVAL '2' DAY | STRING        | INTERVAL DAY | false |

    Scenario Outline: div rejects <case> with BINARY_OP_WRONG_TYPE, ansi <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_WRONG_TYPE\] Cannot resolve ".* div .*" due to data type mismatch: .*not "<type>"\.

      Examples:
        | case               | args                                         | type    | ansi  |
        | DOUBLE pair        | CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)     | DOUBLE  | true  |
        | DOUBLE pair        | CAST(1.0 AS DOUBLE), CAST(2.0 AS DOUBLE)     | DOUBLE  | false |
        | FLOAT pair         | CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)       | FLOAT   | true  |
        | FLOAT pair         | CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)       | FLOAT   | false |
        | BOOLEAN pair       | true, true                                   | BOOLEAN | true  |
        | BOOLEAN pair       | true, true                                   | BOOLEAN | false |
        | DATE pair          | DATE '2024-01-15', DATE '2024-01-16'         | DATE    | true  |
        | DATE pair          | DATE '2024-01-15', DATE '2024-01-16'         | DATE    | false |
        | DECIMAL by DOUBLE  | CAST(1 AS DECIMAL(5,2)), CAST(2.0 AS DOUBLE) | DOUBLE  | true  |
        | DECIMAL by DOUBLE  | CAST(1 AS DECIMAL(5,2)), CAST(2.0 AS DOUBLE) | DOUBLE  | false |
        | DOUBLE by DECIMAL  | CAST(1.0 AS DOUBLE), CAST(2 AS DECIMAL(5,2)) | DOUBLE  | true  |
        | DOUBLE by DECIMAL  | CAST(1.0 AS DOUBLE), CAST(2 AS DECIMAL(5,2)) | DOUBLE  | false |
        | STRING pair        | '10', '3'                                    | STRING  | true  |
        | STRING pair        | '10', '3'                                    | DOUBLE  | false |
        | STRING by DECIMAL  | '5', CAST(2 AS DECIMAL(5,2))                 | DOUBLE  | true  |
        | STRING by DECIMAL  | '5', CAST(2 AS DECIMAL(5,2))                 | DOUBLE  | false |
        | STRING by DOUBLE   | '5', CAST(2.0 AS DOUBLE)                     | DOUBLE  | true  |
        | STRING by DOUBLE   | '5', CAST(2.0 AS DOUBLE)                     | DOUBLE  | false |
        | STRING by DATE     | '5', DATE '2024-01-15'                       | DATE    | true  |
        | STRING by BOOLEAN  | '5', true                                    | BOOLEAN | true  |
        | STRING by NULL     | '5', NULL                                    | STRING  | true  |
        | NULL by STRING     | NULL, '3'                                    | STRING  | true  |
        | STRING by BINARY   | '5', X'01'                                   | BINARY  | true  |

    # `findWiderTypeForString`'s catch-all arm is `case (_: StringType, a: AtomicType)`
    # (AnsiStringPromotionTypeCoercion.scala:104), so a non-atomic peer falls through to `None`
    # and the string is NOT promoted. The legacy rule has no such guard and promotes anyway,
    # which is why the reported left type differs between the two modes.
    Scenario: div rejects a STRING by a non-atomic ARRAY under ANSI without promoting the string
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div('5', array(1)) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve ".* div .*" due to data type mismatch: .*incompatible types \("STRING" and "ARRAY<INT>"\)\.

    Scenario: div rejects a STRING by a non-atomic ARRAY without ANSI after promoting the string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div('5', array(1)) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve ".* div .*" due to data type mismatch: .*incompatible types \("DOUBLE" and "ARRAY<INT>"\)\.

    # The `.*` for the operand SQL in the two Outlines above keeps their tables readable across
    # 50 rows, so these scenarios pin it exactly. Spark renders the clause from `Expression.sql`
    # — `BinaryOperator.sql` is `s"(${left.sql} $sqlOperator ${right.sql})"` (Expression.scala:858)
    # with `sqlOperator = "div"` (arithmetic.scala:908) — and `usePrettyExpression` strips every
    # IMPLICIT cast (util/package.scala:129-130), so the PRE-coercion operand names are the right
    # source. Sail builds them from the same display names the resolver uses to name the output
    # column (resolver/expression/function.rs:291). These cannot be one Outline: a backslash in an
    # Examples cell reaches `re.search` doubled, so each escaped pattern has to stay on its step
    # line. All four values measured on the Spark 4.2.0 JVM.
    Scenario: div names a boolean pair in its rejection message
      When query
        """
        SELECT div(true, true) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_WRONG_TYPE\] Cannot resolve "\(true div true\)" due to data type mismatch:

    # A STRING literal renders UNQUOTED (`usePrettyExpression`, util/package.scala:94).
    Scenario: div names a string literal in its rejection message
      When query
        """
        SELECT div('5', true) AS result
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_WRONG_TYPE\] Cannot resolve "\(5 div true\)" due to data type mismatch:

    Scenario: div names a column in its rejection message
      When query
        """
        SELECT div(a, b) AS result FROM VALUES (CAST(1 AS DOUBLE), 1) AS t(a, b)
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve "\(a div b\)" due to data type mismatch:

    Scenario: div names a nested call in its rejection message
      When query
        """
        SELECT div(abs(a), b) AS result FROM VALUES (CAST(1 AS DOUBLE), 1) AS t(a, b)
        """
      Then query error of class AnalysisException matching ^\[DATATYPE_MISMATCH\.BINARY_OP_DIFF_TYPES\] Cannot resolve "\(abs\(a\) div b\)" due to data type mismatch:

  # The decimal kernel's arms were all pinned on folded literals. These three reach them from a
  # COLUMN, which is the only shape that tells a per-row check from a whole-array one.
  Rule: the DECIMAL kernel decides per row, not per batch

    # `resultDecimalType` floors the precision at 1 when `p1 - s1 + s2` is 0
    # (arithmetic.scala:899-906); the quotient is necessarily 0 in that shape.
    Scenario: div DECIMAL with a zero-integer-digit result precision
      When query
        """
        SELECT div(CAST(0.9 AS DECIMAL(1,1)), CAST(2 AS DECIMAL(2,0))) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: div wide DECIMAL from a column narrows to BIGINT by wrapping
      When query
        """
        SELECT div(a, CAST(1 AS DECIMAL(38,8))) AS result
        FROM VALUES (CAST(12345678901234567890.12345678 AS DECIMAL(38,8))) AS t(a)
        """
      Then query result
        | result               |
        | -6101065172474983726 |

    # Only the second row leaves `resultDecimalType`; a check that aborted the whole array
    # would lose rows 1 and 4, which a single-row fixture cannot distinguish.
    Scenario: div DECIMAL nulls only the offending row under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(a, CAST(0.1 AS DECIMAL(38,37))) AS result FROM VALUES
          (CAST(1 AS DECIMAL(38,0))),
          (CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0))),
          (CAST(NULL AS DECIMAL(38,0))),
          (CAST(2 AS DECIMAL(38,0)))
        AS t(a)
        """
      Then query result ordered
        | result |
        | 10     |
        | NULL   |
        | NULL   |
        | 20     |

  Rule: div reports zero division and overflow exactly as Spark does

    Scenario Outline: div over <family> reports division by zero with no wrapper prefix
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error of class ArithmeticException matching ^\[DIVIDE_BY_ZERO\] Division by zero\. Use `try_divide` to tolerate divisor being 0 and return NULL instead\. If necessary set "spark\.sql\.ansi\.enabled" to "false" to bypass this error\.

      Examples:
        | family        | args                                                 |
        | BIGINT        | CAST(5 AS BIGINT), CAST(0 AS BIGINT)                 |
        | DECIMAL       | CAST(5.5 AS DECIMAL(5,2)), CAST(0.0 AS DECIMAL(5,2)) |
        | INTERVAL DAY  | INTERVAL '10' DAY, INTERVAL '0' DAY                  |
        | INTERVAL YEAR | INTERVAL '10' YEAR, INTERVAL '0' YEAR                |

    Scenario: div reports integral overflow with no wrapper prefix
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query error of class ArithmeticException matching ^\[ARITHMETIC_OVERFLOW\] Overflow in integral divide\. Use 'try_divide' to tolerate overflow and return NULL instead\. If necessary set "spark\.sql\.ansi\.enabled" to "false" to bypass this error\.

    Scenario Outline: DIV coerces every CASE branch before widening with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case
        """
      Then query result
        | dividend_case | divisor_case |
        | 3             | 6            |
      When query
        """
        SELECT id,
               (CASE WHEN id = 0 THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case,
               3 DIV (CASE WHEN id = 0 THEN 1 ELSE 0.5 END) AS divisor_case
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | dividend_case | divisor_case |
        | 0  | 0             | 3            |
        | 1  | 3             | 6            |

      Examples:
        | ansi  |
        | true  |
        | false |


    Scenario Outline: DIV follows projected CASE types through <boundary> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        <query>
        """
      Then query result
        | dividend_case | divisor_case |
        | 3             | 6            |

      Examples:
        | boundary | ansi  | query |
        | derived  | true  | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) t |
        | derived  | false | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) t |
        | CTE      | true  | WITH t AS (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM t |
        | CTE      | false | WITH t AS (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d) SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM t |
        | scalar   | true  | SELECT (SELECT CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case, 3 DIV (SELECT CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case |
        | scalar   | false | SELECT (SELECT CASE WHEN false THEN 0 ELSE 1.5 END) DIV 0.5 AS dividend_case, 3 DIV (SELECT CASE WHEN false THEN 1 ELSE 0.5 END) AS divisor_case |
        | UNION    | true  | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d UNION SELECT 1.5, 0.5) t |
        | UNION    | false | SELECT v DIV 0.5 AS dividend_case, 3 DIV d AS divisor_case FROM (SELECT CASE WHEN false THEN 0 ELSE 1.5 END AS v, CASE WHEN false THEN 1 ELSE 0.5 END AS d UNION SELECT 1.5, 0.5) t |

    Scenario Outline: DIV preserves grouped and shared aggregate expressions with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (id + 0) DIV 2 AS q FROM range(4) GROUP BY id + 0 ORDER BY q
        """
      Then query result ordered
        | q |
        | 0 |
        | 0 |
        | 1 |
        | 1 |
      When query
        """
        SELECT sum(id + 0) AS s, sum(id + 0) DIV 2 AS q
        FROM range(4) HAVING sum(id + 0) > 0
        """
      Then query result
        | s | q |
        | 6 | 3 |
      When query
        """
        SELECT grouping(id + 0) DIV 1 AS g, grouping_id(id + 0) DIV 1 AS gid
        FROM range(2) GROUP BY ROLLUP(id + 0) ORDER BY g
        """
      Then query result ordered
        | g | gid |
        | 0 | 0   |
        | 0 | 0   |
        | 1 | 1   |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: DIV preserves shared ordered windows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT sum(id) OVER (ORDER BY id + 0) AS s,
               sum(id) OVER (ORDER BY id + 0) DIV 2 AS q
        FROM range(4) ORDER BY id
        """
      Then query result ordered
        | s | q |
        | 0 | 0 |
        | 1 | 0 |
        | 3 | 1 |
        | 6 | 3 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Projected <data_type> minimum DIV negative one widens with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT v DIV -1 AS result
        FROM VALUES (CAST(<minimum> AS <data_type>)) AS t(v)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | data_type | minimum     | result     | ansi  |
        | TINYINT   | -128        | 128        | true  |
        | TINYINT   | -128        | 128        | false |
        | SMALLINT  | -32768      | 32768      | true  |
        | SMALLINT  | -32768      | 32768      | false |
        | INT       | -2147483648 | 2147483648 | true  |
        | INT       | -2147483648 | 2147483648 | false |
