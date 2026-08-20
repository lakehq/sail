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

  Rule: Argument count validation

    Scenario: div zero args errors
      When query
        """
        SELECT div() AS result
        """
      Then query error .*

    Scenario: div one arg errors
      When query
        """
        SELECT div(10) AS result
        """
      Then query error .*

    Scenario: div three args errors
      When query
        """
        SELECT div(10, 3, 1) AS result
        """
      Then query error .*

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

    Scenario: div untyped NULL dividend
      When query
        """
        SELECT div(NULL, 5) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Overflow semantics — LONG_MIN div -1
    # Two's-complement: BIGINT range is [-2^63, 2^63-1]; -LONG_MIN overflows
    # BIGINT, so under ANSI=false Spark wraps to LONG_MIN (matches Java
    # Math.floorDiv); ANSI=true raises ARITHMETIC_OVERFLOW.

    Scenario: div LONG_MIN by -1 wraps under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query result
        | result                |
        | -9223372036854775808  |

    Scenario: div LONG_MIN by -1 errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(CAST(-9223372036854775808 AS BIGINT), CAST(-1 AS BIGINT)) AS result
        """
      Then query error Overflow in integral divide

    Scenario: div INT MIN by -1 widens to BIGINT (no overflow)
      When query
        """
        SELECT div(-2147483648, -1) AS result
        """
      Then query result
        | result     |
        | 2147483648 |

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
      Then query error .*

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

    Scenario: div DECIMAL truncates toward zero
      When query
        """
        SELECT div(CAST(10.9 AS DECIMAL(5,2)), CAST(3.0 AS DECIMAL(5,2))) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: div negative DECIMAL truncates toward zero
      When query
        """
        SELECT div(CAST(-10.9 AS DECIMAL(5,2)), CAST(3.0 AS DECIMAL(5,2))) AS result
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

    Scenario: div INTERVAL DAY by zero INTERVAL DAY errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(INTERVAL '10' DAY, INTERVAL '0' DAY) AS result
        """
      Then query error Division by zero

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

    Scenario: div INTERVAL YEAR by zero INTERVAL YEAR errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '0' YEAR) AS result
        """
      Then query error Division by zero

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

    Scenario: div STRING/STRING errors
      When query
        """
        SELECT div('10', '3') AS result
        """
      Then query error .*

    Scenario: div BOOLEAN errors
      When query
        """
        SELECT div(true, true) AS result
        """
      Then query error .*

    Scenario: div DATE errors
      When query
        """
        SELECT div(DATE '2024-01-15', DATE '2024-01-01') AS result
        """
      Then query error .*

    Scenario: div INTERVAL DAY by INT errors
      When query
        """
        SELECT div(INTERVAL '10' DAY, 2) AS result
        """
      Then query error due to data type mismatch

    Scenario: div INTERVAL YEAR by INTERVAL DAY errors
      When query
        """
        SELECT div(INTERVAL '10' YEAR, INTERVAL '2' DAY) AS result
        """
      Then query error due to data type mismatch

    Scenario: div FLOAT/FLOAT errors
      When query
        """
        SELECT div(CAST(1.0 AS FLOAT), CAST(1.0 AS FLOAT)) AS result
        """
      Then query error .*

    Scenario: div DOUBLE/DOUBLE errors
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), CAST(1.0 AS DOUBLE)) AS result
        """
      Then query error .*

    Scenario: div INT/DOUBLE errors
      When query
        """
        SELECT div(10, CAST(3.0 AS DOUBLE)) AS result
        """
      Then query error .*

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

    Scenario: regular division plus cast works with DOUBLE
      When query
        """
        SELECT CAST(CAST(1.5 AS DOUBLE) / CAST(0.3 AS DOUBLE) AS BIGINT) AS result
        """
      Then query result
        | result |
        | 5      |

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
    Scenario: div INT by an unparseable STRING errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(5, 'abc') AS result
        """
      Then query error (?i)cannot (be )?cast

    Scenario: div INT by STRING zero errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(5, '0') AS result
        """
      Then query error Division by zero

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

    Scenario: div DOUBLE by literal zero is rejected under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), 0) AS result
        """
      Then query error .*

    Scenario: div DOUBLE by literal zero DOUBLE is rejected under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(CAST(1.0 AS DOUBLE), CAST(0.0 AS DOUBLE)) AS result
        """
      Then query error .*

    Scenario: div BOOLEAN by literal zero is rejected under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(true, 0) AS result
        """
      Then query error .*

    Scenario: div DATE by literal zero is rejected under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT div(DATE '2024-01-15', 0) AS result
        """
      Then query error .*

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
      Then query error .*

    Scenario: div wide calendar interval does not overflow into a wrong answer
      When query
        """
        SELECT div(make_interval(0, 4000, 0, 1, 0, 0, 0), make_interval(0, 1, 0, 1, 0, 0, 0)) AS result
        """
      Then query error .*

  # Spark builds the default column name from `sqlOperator` through `BinaryOperator.sql`
  # (Expression.scala:859), so an unaliased `div` is named `(7 div 2)`. Sail names it after
  # the planner function instead. Every other scenario in this file aliases with `AS result`,
  # so nothing else covers the generated name.
  Rule: div names an unaliased column the way Spark does

    @sail-bug
    Scenario: an unaliased div column carries Spark's operator name
      When query
        """
        SELECT 7 div 2
        """
      Then query result
        | (7 div 2) |
        | 3         |

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

  # DECIMAL is one of the four operand families Spark's `IntegralDivide` accepts, but it
  # reaches DataFusion arithmetic rather than a Sail kernel, so its zero-divisor policy
  # is built in the planner and needs its own pair. The NULL-dividend scenario pins that
  # a NULL dividend still wins over a zero divisor under ANSI.
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

  # Boundary cases where Sail still diverges. All three are pre-existing and outside the
  # scope of this change, but they are measured against the JVM rather than assumed, so
  # they are pinned here to fail loudly the day they are fixed.
  Rule: Numeric boundaries not yet matching Spark

    # Spark selects `PhysicalIntegerType.integral` for YEAR TO MONTH, so it divides in
    # 32-bit and wraps on `Int.MinValue / -1`, then widens. Sail widens to i64 first and
    # so returns the unwrapped positive value.
    @sail-bug
    Scenario: div YEAR-MONTH interval wraps in 32-bit at the Int boundary
      When query
        """
        SELECT div(make_ym_interval(-178956970, -8), INTERVAL '-1' MONTH) AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    # `checkDivideOverflow` is enabled only when the dividend is LONG, never for an
    # interval, so Spark wraps here instead of raising — in BOTH ANSI modes, since the
    # gate does not consult `failOnError` for this operand type. Sail's day-time path
    # divides through a checked Int64 kernel and raises either way, so both modes are
    # pinned: a fix that only covered the ANSI half would otherwise look complete.
    @sail-bug
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

    # Spark divides on BigDecimal and only then narrows with `Decimal.toLong`, which
    # discards the high-order bits. Sail casts the decimal quotient to BIGINT and the
    # cast rejects the out-of-range value.
    @sail-bug
    Scenario: div wide DECIMAL narrows to BIGINT by wrapping
      When query
        """
        SELECT div(CAST(12345678901234567890.12345678 AS DECIMAL(38,8)), CAST(1 AS DECIMAL(38,8))) AS result
        """
      Then query result
        | result               |
        | -6101065172474983726 |

  # Anchored at the start on purpose: a bare substring also matches a wrapper-prefixed
  # message, so only the anchor discriminates. The pattern must stay on the step line — a
  # backslash in an Examples cell arrives at `re.search` doubled (measured); do not move it.
  Rule: div reports zero division and overflow exactly as Spark does

    Scenario Outline: div over <family> reports division by zero with no wrapper prefix
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT div(<args>) AS result
        """
      Then query error ^\[DIVIDE_BY_ZERO\] Division by zero

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
      Then query error ^\[ARITHMETIC_OVERFLOW\] Overflow in integral divide
