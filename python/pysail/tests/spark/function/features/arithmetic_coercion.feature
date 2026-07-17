@arithmetic_coercion
Feature: Spark type coercion for the +, -, *, /, % operators and string operands

  # DataFusion's BinaryTypeCoercer does not perform these Spark coercions; Sail
  # applies them in the arithmetic plan builders (math.rs) so the *result type*
  # matches Spark 4.1.1. Values and precision/scale are asserted below via typeof.
  #
  # Known remaining gap (see the @sail-bug scenario): Spark marks decimal
  # arithmetic nullable=true (overflow may yield NULL) even for non-null operands,
  # while a native BinaryExpr inherits its operands' nullability (=> false).
  # Matching Spark's nullability needs the custom PhysicalExpr follow-up.

  Rule: Decimal with an integer literal narrows the literal to minimal precision
    Scenario: decimal times a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * 3 AS r
        """
      Then query result
        | t             | r    |
        | decimal(12,2) | 7.50 |

    Scenario: decimal plus a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) + 3) AS t,
               CAST(2.5 AS DECIMAL(10,2)) + 3 AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 5.50 |

    Scenario: decimal minus a single-digit integer literal
      When query
        """
        SELECT typeof(CAST(5.5 AS DECIMAL(10,2)) - 2) AS t,
               CAST(5.5 AS DECIMAL(10,2)) - 2 AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 3.50 |

    Scenario: decimal times a three-digit integer literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 100) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * 100 AS r
        """
      Then query result
        | t             | r      |
        | decimal(14,2) | 250.00 |

    Scenario: a negative integer literal narrows the same as a positive one
      # The literal -3 narrows to Decimal(1,0) (sign does not add a digit), so the
      # result type matches `dec * 3`. Guards against a Negative(Literal) plan node
      # skipping the narrowing.
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * -3) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * -3 AS r
        """
      Then query result
        | t             | r     |
        | decimal(12,2) | -7.50 |

  Rule: Non-decimal, non-string operands need no Spark coercion
    # DataFusion's own coercion already matches Spark for these, so the plan builder
    # leaves them alone. Pinned so a future coercion change cannot silently break them.

    Scenario: integer widths and dates coerce like Spark
      When query
        """
        SELECT typeof(CAST(1 AS TINYINT) + CAST(2 AS TINYINT)) AS byte_byte,
               typeof(CAST(1 AS TINYINT) + CAST(2 AS INT)) AS byte_int,
               typeof(DATE '2024-01-01' + 1) AS date_int
        """
      Then query result
        | byte_byte | byte_int | date_int |
        | tinyint   | int      | date     |

    @sail-bug
    Scenario: date minus date is a day interval
      # Spark returns `interval day`; Sail returns bigint. Not an operand-coercion gap —
      # the operands take no cast — but DataFusion's result type for Date32 - Date32.
      # Fixing it needs a structural rewrite of the subtraction, not a coercion.
      When query
        """
        SELECT typeof(DATE '2024-01-05' - DATE '2024-01-01') AS t
        """
      Then query result
        | t            |
        | interval day |

  Rule: A capped decimal + or - reduces the scale like Spark
    # Spark caps a wide `+`/`-` with adjustPrecisionScale, which REDUCES the scale to keep
    # the integer digits. Arrow caps with a plain min(_, 38) that keeps the scale, so
    # decimal(38,10) + decimal(38,2) came out decimal(38,10) instead of decimal(38,6) —
    # a wrong type under the DEFAULT config. Only the type is re-derived here; the sum
    # itself stays on the native kernel.

    Scenario: a wide decimal sum reduces the scale to the adjusted one
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,2))) AS t,
               CAST(1.005 AS DECIMAL(38,10)) + CAST(2.5 AS DECIMAL(38,2)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 3.505000 |

    Scenario: a wide decimal difference reduces the scale to the adjusted one
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,10)) - CAST(1 AS DECIMAL(38,2))) AS t
        """
      Then query result
        | t             |
        | decimal(38,6) |

    Scenario: a decimal plus an integer literal reduces the scale when it overflows 38
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,18)) + 2) AS t
        """
      Then query result
        | t              |
        | decimal(38,17) |

    Scenario: a decimal sum that fits in 38 keeps the native type
      # Guards the gate: below precision 38 Arrow already agrees with Spark, so the
      # re-typing must not fire.
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2))) AS t,
               CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 6.00 |

  Rule: Float or double combined with a decimal promotes to double
    Scenario: float times decimal returns double
      When query
        """
        SELECT typeof(CAST(1.5 AS FLOAT) * CAST(2.0 AS DECIMAL(10,2))) AS t,
               CAST(1.5 AS FLOAT) * CAST(2.0 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r   |
        | double | 3.0 |

  Rule: String operand in arithmetic coerces to a numeric type
    # DataFusion rejects string arithmetic; Spark coerces the string. ANSI off promotes
    # both operands to double, and a string that does not parse yields NULL. ANSI on
    # promotes to BIGINT against an integral operand and to DOUBLE against a fractional
    # one (float or decimal), casting strictly; string + string and string + NULL are
    # rejected under ANSI.

    Scenario: string plus integer, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + 3) AS t, '5' + 3 AS r
        """
      Then query result
        | t      | r   |
        | double | 8.0 |

    Scenario: string plus decimal-typed literal promotes to double
      When query
        """
        SELECT typeof('5' + 3.5) AS t, '5' + 3.5 AS r
        """
      Then query result
        | t      | r   |
        | double | 8.5 |

    Scenario: string plus decimal column promotes to double
      When query
        """
        SELECT typeof('5' + CAST(2.5 AS DECIMAL(10,2))) AS t,
               '5' + CAST(2.5 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r   |
        | double | 7.5 |

    Scenario: string plus string, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + '3') AS t, '5' + '3' AS r
        """
      Then query result
        | t      | r   |
        | double | 8.0 |

  Rule: Decimal modulo narrows an integer literal like Spark
    Scenario: decimal modulo a single-digit integer literal
      # Literal 3 narrows to Decimal(1,0); Spark remainder rule (scale=max(s1,s2),
      # precision=min(p1-s1,p2-s2)+scale) then gives decimal(3,2), not decimal(10,2).
      When query
        """
        SELECT typeof(CAST(10.5 AS DECIMAL(10,2)) % 3) AS t,
               CAST(10.5 AS DECIMAL(10,2)) % 3 AS r
        """
      Then query result
        | t            | r    |
        | decimal(3,2) | 1.50 |

  Rule: Modulo of decimals follows Spark's remainder rule and sign
    # Spark remainder type: scale = max(s1,s2), precision = min(p1-s1, p2-s2) + scale.
    # The result takes the sign of the dividend. Unlike division, an integer column
    # widens to Decimal(10,0) but min(p1-s1, ...) means it does not change the type.

    Scenario: decimal modulo decimal
      When query
        """
        SELECT typeof(CAST(10.5 AS DECIMAL(10,2)) % CAST(3.0 AS DECIMAL(10,2))) AS t,
               CAST(10.5 AS DECIMAL(10,2)) % CAST(3.0 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t             | r    |
        | decimal(10,2) | 1.50 |

    Scenario: decimal modulo decimal with different scales
      When query
        """
        SELECT typeof(CAST(10.567 AS DECIMAL(10,3)) % CAST(3.1 AS DECIMAL(10,1))) AS t,
               CAST(10.567 AS DECIMAL(10,3)) % CAST(3.1 AS DECIMAL(10,1)) AS r
        """
      Then query result
        | t             | r     |
        | decimal(10,3) | 1.267 |

    Scenario: modulo takes the sign of the dividend
      When query
        """
        SELECT CAST(-10.5 AS DECIMAL(10,2)) % CAST(3.0 AS DECIMAL(10,2)) AS a,
               CAST(10.5 AS DECIMAL(10,2)) % CAST(-3.0 AS DECIMAL(10,2)) AS b,
               CAST(-10.5 AS DECIMAL(10,2)) % CAST(-3.0 AS DECIMAL(10,2)) AS c
        """
      Then query result
        | a     | b    | c     |
        | -1.50 | 1.50 | -1.50 |

    Scenario: a NULL operand makes modulo NULL
      When query
        """
        SELECT typeof(CAST(NULL AS DECIMAL(10,2)) % CAST(3 AS DECIMAL(10,2))) AS t,
               CAST(NULL AS DECIMAL(10,2)) % CAST(3 AS DECIMAL(10,2)) AS a,
               CAST(10 AS DECIMAL(10,2)) % CAST(NULL AS DECIMAL(10,2)) AS b
        """
      Then query result
        | t             | a    | b    |
        | decimal(10,2) | NULL | NULL |

    Scenario: double modulo a decimal promotes to double
      When query
        """
        SELECT typeof(CAST(1.5 AS DOUBLE) % CAST(2.0 AS DECIMAL(10,2))) AS t,
               CAST(1.5 AS DOUBLE) % CAST(2.0 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r   |
        | double | 1.5 |

    Scenario: decimal modulo an integer column
      When query
        """
        SELECT typeof(a % b) AS t, a % b AS r
        FROM VALUES
          (CAST(10.5 AS DECIMAL(10,2)), CAST(3 AS INT)),
          (CAST(7.5 AS DECIMAL(10,2)), CAST(2 AS INT))
          AS t(a, b)
        ORDER BY b
        """
      Then query result ordered
        | t             | r    |
        | decimal(10,2) | 1.50 |
        | decimal(10,2) | 1.50 |

    Scenario: modulo of decimal columns over multiple rows
      When query
        """
        SELECT a % b AS r
        FROM VALUES
          (CAST(10.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(7.5 AS DECIMAL(10,2)), CAST(2.0 AS DECIMAL(10,2))),
          (CAST(-8.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2)))
          AS t(a, b)
        ORDER BY a
        """
      Then query result ordered
        | r     |
        | -2.50 |
        | 1.50  |
        | 1.50  |

  Rule: Decimal division uses Spark's scale rule and HALF_UP rounding
    # Spark: scale = max(6, s1 + p2 + 1), precision = (p1 - s1) + s2 + scale, then
    # adjustPrecisionScale (cap at 38); the value is HALF_UP-rounded to that scale.
    # DataFusion's Arrow `div` uses scale s1+4 and truncates, so it diverges.

    Scenario: decimal divided by decimal
      When query
        """
        SELECT typeof(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2))) AS t,
               CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t              | r             |
        | decimal(23,13) | 3.3333333333333 |

    Scenario: decimal division rounds HALF_UP on the last digit
      # 2/3 = 0.6666…; at scale 13 the guard digit forces the last digit up to 7.
      When query
        """
        SELECT CAST(2 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | r               |
        | 0.6666666666667 |

    Scenario: decimal division capped at precision 38
      When query
        """
        SELECT typeof(CAST(1.0 AS DECIMAL(38,10)) / CAST(3.0 AS DECIMAL(10,5))) AS t,
               CAST(1.0 AS DECIMAL(38,10)) / CAST(3.0 AS DECIMAL(10,5)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 0.333333 |

    Scenario: decimal divided by an integer literal narrows the literal
      When query
        """
        SELECT typeof(CAST(10.2 AS DECIMAL(10,2)) / 3) AS t,
               CAST(10.2 AS DECIMAL(10,2)) / 3 AS r
        """
      Then query result
        | t             | r        |
        | decimal(14,6) | 3.400000 |

  Rule: Decimal division edge cases — NULL, special values, multiple rows
    # ANSI-independent behavior. Zero-divisor (NULL vs error) is ANSI-specific and
    # is covered in divide_by_zero.feature, not here.

    Scenario: a NULL operand makes decimal division NULL
      When query
        """
        SELECT typeof(CAST(NULL AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2))) AS t,
               CAST(NULL AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2)) AS a,
               CAST(10 AS DECIMAL(10,2)) / CAST(NULL AS DECIMAL(10,2)) AS b,
               CAST(NULL AS DECIMAL(10,2)) / CAST(NULL AS DECIMAL(10,2)) AS c
        """
      Then query result
        | t              | a    | b    | c    |
        | decimal(23,13) | NULL | NULL | NULL |

    Scenario: IEEE special values propagate through double division
      # Divisor is non-zero, so the double arm applies IEEE semantics (Spark only
      # overrides IEEE for a zero divisor).
      When query
        """
        SELECT typeof(CAST('NaN' AS DOUBLE) / CAST(2.0 AS DOUBLE)) AS t,
               CAST('NaN' AS DOUBLE) / CAST(2.0 AS DOUBLE) AS a,
               CAST(6.0 AS DOUBLE) / CAST('Infinity' AS DOUBLE) AS b
        """
      Then query result
        | t      | a   | b   |
        | double | NaN | 0.0 |

    Scenario: decimal column divided by decimal column over multiple rows
      When query
        """
        SELECT typeof(a / b) AS t, a / b AS r
        FROM VALUES
          (CAST(10.00 AS DECIMAL(10,2)), CAST(3.00 AS DECIMAL(10,2))),
          (CAST(7.00 AS DECIMAL(10,2)), CAST(2.00 AS DECIMAL(10,2))),
          (CAST(1.00 AS DECIMAL(10,2)), CAST(7.00 AS DECIMAL(10,2))),
          (CAST(-5.00 AS DECIMAL(10,2)), CAST(3.00 AS DECIMAL(10,2)))
          AS t(a, b)
        ORDER BY a
        """
      Then query result ordered
        | t              | r                |
        | decimal(23,13) | -1.6666666666667 |
        | decimal(23,13) | 0.1428571428571  |
        | decimal(23,13) | 3.5000000000000  |
        | decimal(23,13) | 3.3333333333333  |

    Scenario: division sign follows the operands, magnitude rounds HALF_UP
      When query
        """
        SELECT typeof(CAST(-10.00 AS DECIMAL(10,2)) / CAST(-3.00 AS DECIMAL(10,2))) AS t,
               CAST(-10.00 AS DECIMAL(10,2)) / CAST(-3.00 AS DECIMAL(10,2)) AS a,
               CAST(10.00 AS DECIMAL(10,2)) / CAST(-3.00 AS DECIMAL(10,2)) AS b,
               CAST(-10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS c
        """
      Then query result
        | t              | a               | b                | c                |
        | decimal(23,13) | 3.3333333333333 | -3.3333333333333 | -3.3333333333333 |

    Scenario: exact decimal division keeps trailing zeros at the promoted scale
      When query
        """
        SELECT typeof(CAST(10.00 AS DECIMAL(10,2)) / CAST(4.00 AS DECIMAL(10,2))) AS t,
               CAST(10.00 AS DECIMAL(10,2)) / CAST(4.00 AS DECIMAL(10,2)) AS a,
               CAST(6.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS b,
               CAST(9.00 AS DECIMAL(10,2)) / CAST(4.00 AS DECIMAL(10,2)) AS c
        """
      Then query result
        | t              | a               | b               | c               |
        | decimal(23,13) | 2.5000000000000 | 2.0000000000000 | 2.2500000000000 |

    Scenario: division result scale follows both operands' precision and scale
      When query
        """
        SELECT typeof(CAST(1.2345 AS DECIMAL(5,4)) / CAST(6.7890 AS DECIMAL(5,4))) AS t1,
               CAST(1.2345 AS DECIMAL(5,4)) / CAST(6.7890 AS DECIMAL(5,4)) AS r1,
               typeof(CAST(0.00001 AS DECIMAL(10,8)) / CAST(3.00 AS DECIMAL(10,2))) AS t2,
               CAST(0.00001 AS DECIMAL(10,8)) / CAST(3.00 AS DECIMAL(10,2)) AS r2
        """
      Then query result
        | t1             | r1           | t2             | r2                    |
        | decimal(15,10) | 0.1818382678 | decimal(23,19) | 0.0000033333333333333 |

  Rule: Decimal divided by an integer column uses the integer's type-based decimal
    # Spark casts an integer *column* to DecimalType.forType (Int -> Decimal(10,0),
    # Byte -> Decimal(3,0), Short -> Decimal(5,0), Long -> Decimal(20,0)) — distinct
    # from an integer literal, which narrows to its minimal decimal.

    Scenario: decimal divided by an INT column
      When query
        """
        SELECT typeof(a / b) AS t, a / b AS r
        FROM VALUES
          (CAST(10.00 AS DECIMAL(10,2)), CAST(3 AS INT)),
          (CAST(7.00 AS DECIMAL(10,2)), CAST(2 AS INT))
          AS t(a, b)
        ORDER BY b
        """
      Then query result ordered
        | t              | r               |
        | decimal(21,13) | 3.5000000000000 |
        | decimal(21,13) | 3.3333333333333 |

    Scenario: INT column divided by a decimal
      When query
        """
        SELECT typeof(a / b) AS t, a / b AS r
        FROM VALUES
          (CAST(10 AS INT), CAST(3.00 AS DECIMAL(10,2))),
          (CAST(7 AS INT), CAST(2.00 AS DECIMAL(10,2)))
          AS t(a, b)
        ORDER BY a
        """
      Then query result ordered
        | t              | r            |
        | decimal(23,11) | 3.50000000000 |
        | decimal(23,11) | 3.33333333333 |

    Scenario: division result type depends on the integer column width
      When query
        """
        SELECT typeof(d / bi) AS t_bigint,
               typeof(d / ti) AS t_tinyint,
               typeof(d / si) AS t_smallint
        FROM VALUES
          (CAST(10.00 AS DECIMAL(10,2)), CAST(3 AS BIGINT), CAST(3 AS TINYINT), CAST(3 AS SMALLINT))
          AS t(d, bi, ti, si)
        """
      Then query result
        | t_bigint       | t_tinyint     | t_smallint    |
        | decimal(31,23) | decimal(14,6) | decimal(16,8) |

  Rule: round() of a decimal division survives expression simplification
    # The decimal divide builds a nested round() (HALF_UP to the division scale).
    # Wrapping it in the query's own round() yields Spark's VALUE after
    # SimplifyExpressions and the logical/physical optimizers run — the inner round
    # is not collapsed (this is the point: division + simplify are correct).
    # The result TYPE follows Spark's `RoundBase.dataType` — decimal(11+n, n), not the
    # input's decimal(23, n) — which `round` now derives in the plan builder via the
    # `round_decimal_base` helper that `ceil`/`floor` already used.

    Scenario: round of a decimal division to a smaller scale
      When query
        """
        SELECT typeof(round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 2)) AS t,
               round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 2) AS r
        """
      Then query result
        | t             | r    |
        | decimal(13,2) | 3.33 |

    Scenario: round of a division rounds HALF_UP after dividing
      When query
        """
        SELECT typeof(round(CAST(2.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 4)) AS t,
               round(CAST(2.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 4) AS r
        """
      Then query result
        | t             | r      |
        | decimal(15,4) | 0.6667 |

    Scenario: round of a negative decimal division
      When query
        """
        SELECT typeof(round(CAST(-5.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 6)) AS t,
               round(CAST(-5.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 6) AS r
        """
      Then query result
        | t             | r         |
        | decimal(17,6) | -1.666667 |

    Scenario: round of a decimal division to zero scale
      When query
        """
        SELECT typeof(round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 0)) AS t,
               round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 0) AS r
        """
      Then query result
        | t             | r |
        | decimal(11,0) | 3 |

  Rule: Integer overflow wraps under ANSI off and raises under ANSI on (known gap)
    # ANSI off agrees: both engines wrap, which is Spark's documented behaviour.
    # ANSI on does not: Spark raises, while Sail's native BinaryExpr keeps wrapping and
    # silently returns the wrong number. Overflow on the native BinaryExpr is scoped as
    # future work for the custom PhysicalExpr, so these pin the gap rather than fix it.

    Scenario: integer types wrap on overflow under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT) AS byte_r,
               CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT) AS short_r,
               CAST(2147483647 AS INT) + CAST(1 AS INT) AS int_r,
               CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS long_r,
               CAST(2147483647 AS INT) * CAST(2 AS INT) AS mul_r
        """
      Then query result
        | byte_r | short_r | int_r       | long_r               | mul_r |
        | -128   | -32768  | -2147483648 | -9223372036854775808 | -2    |

    Scenario: integer columns wrap on overflow under ANSI off over multiple rows
      # The literal scenario above is constant-folded at plan time, so only this one
      # actually exercises the runtime kernel over a batch.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a + b AS r
        FROM VALUES
          (CAST(2147483647 AS INT), CAST(1 AS INT)),
          (CAST(1 AS INT), CAST(1 AS INT)),
          (CAST(2147483647 AS INT), CAST(2 AS INT))
          AS t(a, b)
        """
      Then query result
        | r           |
        | -2147483648 |
        | 2           |
        | -2147483647 |

    @sail-bug
    Scenario: an int column sum that overflows raises under ANSI on
      # Column form of the rule below: guards against the divergence being an artifact of
      # constant folding. Spark: [ARITHMETIC_OVERFLOW]; Sail wraps silently.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a + b AS r
        FROM VALUES (CAST(2147483647 AS INT), CAST(1 AS INT)) AS t(a, b)
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: a byte sum that overflows raises under ANSI on
      # Spark: [BINARY_ARITHMETIC_OVERFLOW]; Sail wraps to -128.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: a short sum that overflows raises under ANSI on
      # Spark: [BINARY_ARITHMETIC_OVERFLOW]; Sail wraps to -32768.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: an int sum that overflows raises under ANSI on
      # Spark: [ARITHMETIC_OVERFLOW] integer overflow; Sail wraps to -2147483648.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: a long sum that overflows raises under ANSI on
      # Spark: [ARITHMETIC_OVERFLOW] long overflow; Sail wraps.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: an int product that overflows raises under ANSI on
      # Spark: [ARITHMETIC_OVERFLOW] integer overflow; Sail wraps to -2.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

  Rule: A capped decimal + or - overflows to NULL under ANSI off (known gap)
    # Spark wraps a capped decimal result in CheckOverflow(nullOnOverflow = !ansi), so an
    # overflowing sum is NULL under ANSI off. Arrow's checked kernel raises in both modes,
    # so Sail errors instead. `*` already agrees, because its precision-38 capping path
    # narrows with try_cast; `+`/`-` have no such path.
    #
    # Deliberately left to the custom PhysicalExpr follow-up rather than fixed here:
    # overflow on the native BinaryExpr is scoped as future work, with this PR limited to
    # type coercion.

    Scenario: a capped decimal sum that overflows raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query error (?i)overflow

    @sail-bug
    Scenario: a capped decimal sum that overflows is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: a capped decimal difference that overflows raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(-9e37 AS DECIMAL(38,0)) - CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query error (?i)overflow

    @sail-bug
    Scenario: a capped decimal difference that overflows is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(-9e37 AS DECIMAL(38,0)) - CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: a capped decimal sum that fits is exact
      # The literal is spelled out rather than written `1e37`, which is a DOUBLE and
      # would not survive the cast exactly — that is a separate concern from the capping.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0))
                      + CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0))) AS t,
               CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0))
               + CAST(10000000000000000000000000000000000000 AS DECIMAL(38,0)) AS r
        """
      Then query result
        | t             | r                                      |
        | decimal(38,0) | 20000000000000000000000000000000000000 |

    Scenario: an ordinary decimal sum is untouched by the capped path
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2))) AS t,
               CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t             | r    |
        | decimal(11,2) | 6.00 |

  Rule: Decimal arithmetic is nullable in Spark (known gap — needs custom PhysicalExpr)
    # Spark marks decimal +, -, * as nullable=true even for non-null operands,
    # because the operation can overflow to NULL. A native BinaryExpr built in the
    # plan builder inherits nullability from its operands, so Sail reports false.
    @sail-bug
    Scenario: decimal arithmetic reports nullable=true like Spark
      When query
        """
        SELECT CAST(2.5 AS DECIMAL(10,2)) * 3 AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(12,2) (nullable = true)
        """

  Rule: Decimal multiply caps precision at 38 with Spark's adjustPrecisionScale
    # When p1+p2+1 > 38 Spark caps precision at 38 and REDUCES the scale to
    # max(38 - intDigits, min(scale, 6)), HALF_UP-rounding the value. DataFusion keeps
    # the full scale. Non-capped products (the common case) are exact and unchanged.

    Scenario: capped multiply reduces the scale to the minimum adjusted scale
      When query
        """
        SELECT typeof(CAST(1.0 AS DECIMAL(38,10)) * CAST(2.0 AS DECIMAL(10,5))) AS t,
               CAST(1.0 AS DECIMAL(38,10)) * CAST(2.0 AS DECIMAL(10,5)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 2.000000 |

    Scenario: capped multiply rounds HALF_UP with carry
      # 9.99999999 * 9.99999 = 99.9998999000...; HALF_UP to 6 digits carries to 99.999900.
      When query
        """
        SELECT typeof(CAST(9.99999999 AS DECIMAL(38,10)) * CAST(9.99999 AS DECIMAL(10,5))) AS t,
               CAST(9.99999999 AS DECIMAL(38,10)) * CAST(9.99999 AS DECIMAL(10,5)) AS r
        """
      Then query result
        | t             | r         |
        | decimal(38,6) | 99.999900 |

    Scenario: capped multiply rounds HALF_UP by magnitude when negative
      When query
        """
        SELECT CAST(-1.23456789 AS DECIMAL(38,10)) * CAST(2.11111 AS DECIMAL(10,5)) AS a,
               CAST(1.23456789 AS DECIMAL(38,10)) * CAST(2.11111 AS DECIMAL(10,5)) AS b
        """
      Then query result
        | a         | b        |
        | -2.606309 | 2.606309 |

    Scenario: capped multiply keeps a larger adjusted scale when it fits
      # p=41, scale 20 => adjusted scale max(38-21, 6) = 17.
      When query
        """
        SELECT typeof(CAST(1.5 AS DECIMAL(20,10)) * CAST(2.5 AS DECIMAL(20,10))) AS t,
               CAST(1.5 AS DECIMAL(20,10)) * CAST(2.5 AS DECIMAL(20,10)) AS r
        """
      Then query result
        | t              | r                   |
        | decimal(38,17) | 3.75000000000000000 |

    Scenario: capped multiply reduces the scale to zero
      When query
        """
        SELECT typeof(CAST(1.0 AS DECIMAL(38,0)) * CAST(2.0 AS DECIMAL(10,0))) AS t,
               CAST(1.0 AS DECIMAL(38,0)) * CAST(2.0 AS DECIMAL(10,0)) AS r
        """
      Then query result
        | t             | r |
        | decimal(38,0) | 2 |

    Scenario: capped multiply of a very wide product uses i256
      When query
        """
        SELECT typeof(CAST(1.23 AS DECIMAL(38,20)) * CAST(4.56 AS DECIMAL(38,20))) AS t,
               CAST(1.23 AS DECIMAL(38,20)) * CAST(4.56 AS DECIMAL(38,20)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 5.608800 |

    Scenario: a NULL operand makes a capped multiply NULL
      When query
        """
        SELECT typeof(CAST(NULL AS DECIMAL(38,10)) * CAST(2.0 AS DECIMAL(10,5))) AS t,
               CAST(NULL AS DECIMAL(38,10)) * CAST(2.0 AS DECIMAL(10,5)) AS a,
               CAST(2.0 AS DECIMAL(38,10)) * CAST(NULL AS DECIMAL(10,5)) AS b
        """
      Then query result
        | t             | a    | b    |
        | decimal(38,6) | NULL | NULL |

    Scenario: capped multiply of a decimal by an integer column
      When query
        """
        SELECT typeof(a * b) AS t, a * b AS r
        FROM VALUES (CAST(1.5 AS DECIMAL(38,10)), CAST(2 AS INT)) AS t(a, b)
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 3.000000 |

    Scenario: capped multiply over multiple rows
      When query
        """
        SELECT a * b AS r
        FROM VALUES
          (CAST(1.0 AS DECIMAL(38,10)), CAST(2.0 AS DECIMAL(10,5))),
          (CAST(3.5 AS DECIMAL(38,10)), CAST(2.0 AS DECIMAL(10,5))),
          (CAST(-1.5 AS DECIMAL(38,10)), CAST(2.0 AS DECIMAL(10,5)))
          AS t(a, b)
        ORDER BY a
        """
      Then query result ordered
        | r         |
        | -3.000000 |
        | 2.000000  |
        | 7.000000  |

  Rule: Decimal multiply (non-capped) and special values
    Scenario: decimal multiply decimal keeps the exact product type
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * CAST(3.0 AS DECIMAL(10,2))) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * CAST(3.0 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t             | r      |
        | decimal(21,4) | 7.5000 |

    Scenario: multiply sign combinations
      When query
        """
        SELECT CAST(-2.5 AS DECIMAL(10,2)) * CAST(3.0 AS DECIMAL(10,2)) AS a,
               CAST(2.5 AS DECIMAL(10,2)) * CAST(-3.0 AS DECIMAL(10,2)) AS b
        """
      Then query result
        | a       | b       |
        | -7.5000 | -7.5000 |

    Scenario: decimal multiply over multiple rows with a NULL
      When query
        """
        SELECT a * b AS r
        FROM VALUES
          (CAST(2.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(NULL AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(-2.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2)))
          AS t(a, b)
        """
      Then query result
        | r       |
        | -7.5000 |
        | 7.5000  |
        | NULL    |

    Scenario: IEEE special values propagate through double multiply
      When query
        """
        SELECT typeof(CAST('Infinity' AS DOUBLE) * CAST(2.0 AS DOUBLE)) AS t,
               CAST('Infinity' AS DOUBLE) * CAST(2.0 AS DOUBLE) AS a,
               CAST('NaN' AS DOUBLE) * CAST(2.0 AS DOUBLE) AS b,
               CAST('Infinity' AS DOUBLE) * CAST(0.0 AS DOUBLE) AS c,
               CAST('Infinity' AS DOUBLE) * CAST(2.0 AS DECIMAL(10,2)) AS d
        """
      Then query result
        | t      | a        | b   | c   | d        |
        | double | Infinity | NaN | NaN | Infinity |

  Rule: Further Spark coercion divergences (bug-hunt, not yet implemented)
    # Validated against Spark 4.1.1.

    Scenario: ANSI string plus integer widens to bigint like Spark
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + 3) AS t
        """
      Then query result
        | t      |
        | bigint |

    # `pmod` is a UDF (SparkPmod) but now takes the same operand coercion as the
    # operators, which fixes the float x decimal promotion. What is left diverging is
    # inside the UDF itself: it derives its own result type rather than applying
    # Spark's remainder rule (`min(p1-s1, p2-s2) + max(s1,s2)`), and its NULL
    # handling. Follow-up: fix `SparkPmod`'s return type and NULL handling.
    # `div` (integer division) matches Spark and needs no coercion (always BIGINT).

    Scenario: pmod narrows an integer literal like the remainder rule
      When query
        """
        SELECT typeof(pmod(CAST(10.5 AS DECIMAL(10,2)), 3)) AS t
        """
      Then query result
        | t            |
        | decimal(3,2) |

    Scenario: pmod with a NULL operand returns NULL
      # Spark returns NULL; Sail errors ("Null and Int32 are not coercible").
      When query
        """
        SELECT pmod(NULL, 3) AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: pmod of a double and a decimal promotes to double
      When query
        """
        SELECT typeof(pmod(CAST(1.5 AS DOUBLE), CAST(2.0 AS DECIMAL(10,2)))) AS t
        """
      Then query result
        | t      |
        | double |

  Rule: pmod narrowing to the remainder type honours ANSI
    # `pmod` adds the divisor back (`a % n + n`), so unlike `%` its result is bounded only
    # by |n| — not by the dividend. The remainder type takes min(p1-s1, p2-s2), so the
    # value can exceed it: typed decimal(3,2) here, but reaching 99994.00. Narrowing to
    # that type therefore takes the same ANSI gate as `*` and `/`.

    # Both engines raise here, so the coercion side already agrees — what diverges is only
    # the error itself: Spark names the NUMERIC_VALUE_OUT_OF_RANGE class, Sail reports the
    # Arrow overflow ("99994.00 is too large to store in a Decimal128 of precision 3").
    # Asserted as Spark's class, so it xfails until Sail grows Spark error classes, rather
    # than as a substring both happen to share (which would only assert "some error").
    @sail-bug
    Scenario: a pmod result that overflows its remainder type raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(CAST(-5.00 AS DECIMAL(3,2)), CAST(99999 AS DECIMAL(5,0))) AS r
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

    Scenario: a pmod result that overflows its remainder type is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(pmod(CAST(-5.00 AS DECIMAL(3,2)), CAST(99999 AS DECIMAL(5,0)))) AS t,
               pmod(CAST(-5.00 AS DECIMAL(3,2)), CAST(99999 AS DECIMAL(5,0))) AS r
        """
      Then query result
        | t            | r    |
        | decimal(3,2) | NULL |

  Rule: The float x decimal promotion holds for columns, not just literals
    # This is the promotion the plan builder fixes, so a literal-only test would leave the
    # runtime path unproven: a literal pair is constant-folded away and the kernel never
    # runs over a real batch.

    Scenario: float or double times a decimal column over multiple rows
      When query
        """
        SELECT typeof(f * d) AS ft, f * d AS fr, typeof(g * d) AS gt, g * d AS gr
        FROM VALUES
          (CAST(1.5 AS FLOAT), CAST(2.0 AS DECIMAL(10,2)), CAST(1.5 AS DOUBLE)),
          (CAST(-2.5 AS FLOAT), CAST(4.0 AS DECIMAL(10,2)), CAST(-2.5 AS DOUBLE)),
          (CAST(NULL AS FLOAT), CAST(2.0 AS DECIMAL(10,2)), CAST(NULL AS DOUBLE))
          AS t(f, d, g)
        """
      Then query result
        | ft     | fr    | gt     | gr    |
        | double | 3.0   | double | 3.0   |
        | double | -10.0 | double | -10.0 |
        | double | NULL  | double | NULL  |

    Scenario: a float column plus, minus and divided by a decimal column
      When query
        """
        SELECT typeof(f + d) AS a, typeof(f - d) AS b, typeof(f / d) AS c
        FROM VALUES (CAST(1.5 AS FLOAT), CAST(2.0 AS DECIMAL(10,2))) AS t(f, d)
        """
      Then query result
        | a      | b      | c      |
        | double | double | double |

    Scenario: an Infinity float column times a decimal column stays double
      # The promotion is what keeps this from being cast into a decimal, where Infinity
      # has no representation.
      When query
        """
        SELECT f * d AS r
        FROM VALUES (CAST('Infinity' AS FLOAT), CAST(2.0 AS DECIMAL(10,2))) AS t(f, d)
        """
      Then query result
        | r        |
        | Infinity |

  Rule: pmod and round hold over columns, not just literals

    Scenario: pmod with a NULL in a column over multiple rows
      When query
        """
        SELECT typeof(pmod(a, b)) AS t, pmod(a, b) AS r
        FROM VALUES
          (CAST(10.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(-10.5 AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(NULL AS DECIMAL(10,2)), CAST(3.0 AS DECIMAL(10,2))),
          (CAST(10.5 AS DECIMAL(10,2)), CAST(NULL AS DECIMAL(10,2)))
          AS t(a, b)
        """
      Then query result
        | t             | r    |
        | decimal(10,2) | 1.50 |
        | decimal(10,2) | 1.50 |
        | decimal(10,2) | NULL |
        | decimal(10,2) | NULL |

    Scenario: round of a decimal column keeps Spark's type over multiple rows
      When query
        """
        SELECT typeof(round(a, 2)) AS t, round(a, 2) AS r
        FROM VALUES
          (CAST(3.14159 AS DECIMAL(10,5))),
          (CAST(-2.71828 AS DECIMAL(10,5))),
          (CAST(NULL AS DECIMAL(10,5)))
          AS t(a)
        """
      Then query result
        | t            | r     |
        | decimal(8,2) | 3.14  |
        | decimal(8,2) | -2.72 |
        | decimal(8,2) | NULL  |

  Rule: A BYTE literal is not narrowed — Spark narrows only Short, Int and Long
    # `DecimalType.fromLiteral` narrows Short/Int/Long literals to their minimal
    # decimal, but a Byte literal falls through to `forType(ByteType)` = decimal(3,0).
    # The difference is only observable for `*`, whose result precision depends on the
    # narrowed operand's precision.

    Scenario: decimal times a byte literal uses the type-based decimal(3,0)
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3Y) AS t,
               CAST(2.5 AS DECIMAL(10,2)) * 3Y AS r
        """
      Then query result
        | t             | r    |
        | decimal(14,2) | 7.50 |

    Scenario: an int literal still narrows, unlike a byte literal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3) AS t,
               typeof(CAST(2.5 AS DECIMAL(10,2)) * 3S) AS s
        """
      Then query result
        | t             | s             |
        | decimal(12,2) | decimal(12,2) |

    Scenario: a byte literal narrows the same as an int literal for plus and divide
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) + 3Y) AS p,
               typeof(CAST(2.5 AS DECIMAL(10,2)) / 3Y) AS d
        """
      Then query result
        | p             | d             |
        | decimal(11,2) | decimal(14,6) |

  Rule: ANSI-on string promotion targets BIGINT or DOUBLE, not the peer's exact type
    # Spark's AnsiTypeCoercion promotes a string against an integral operand to BIGINT
    # and against a fractional one to DOUBLE. Casting to the peer's exact type instead
    # would wrongly narrow the valid range (e.g. a value beyond INT range).

    Scenario: string plus an int column, ANSI on, promotes to bigint
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + CAST(1 AS INT)) AS t, '5' + CAST(1 AS INT) AS r
        """
      Then query result
        | t      | r |
        | bigint | 6 |

    Scenario: string plus an int column, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + CAST(1 AS INT)) AS t, '5' + CAST(1 AS INT) AS r
        """
      Then query result
        | t      | r   |
        | double | 6.0 |

    Scenario: a string beyond INT range survives because the target is bigint
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT '3000000000' + CAST(1 AS INT) AS r
        """
      Then query result
        | r          |
        | 3000000001 |

    Scenario: a string beyond INT range, ANSI off, survives as a double
      # Only the type is asserted: rendering the value would drag in an unrelated known
      # gap (Spark prints large doubles in E notation, Sail does not). The ANSI-on pair
      # above pins the value, where the result is a bigint and both agree.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('3000000000' + CAST(1 AS INT)) AS t,
               '3000000000' + CAST(1 AS INT) = 3000000001 AS eq
        """
      Then query result
        | t      | eq   |
        | double | true |

    Scenario: string times a byte column, ANSI on, still promotes to bigint
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' * CAST(2 AS TINYINT)) AS t, '5' * CAST(2 AS TINYINT) AS r
        """
      Then query result
        | t      | r  |
        | bigint | 10 |

    Scenario: string plus a float column, ANSI on, promotes to double
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + CAST(1.5 AS FLOAT)) AS t
        """
      Then query result
        | t      |
        | double |

    Scenario: string plus a decimal column, ANSI on, promotes to double
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + CAST(2.5 AS DECIMAL(10,2))) AS t,
               '5' + CAST(2.5 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r   |
        | double | 7.5 |

  Rule: ANSI-on string promotion over every numeric type
    # The split is integral vs fractional, not the peer's own width: every integral type
    # promotes to BIGINT and every fractional one to DOUBLE. Covering each type guards
    # against a rule that happens to be right only for INT.

    Scenario: a string with each integral type, ANSI on, promotes to bigint
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + CAST(1 AS TINYINT)) AS byte_t,
               typeof('5' + CAST(1 AS SMALLINT)) AS short_t,
               typeof('5' + CAST(1 AS INT)) AS int_t,
               typeof('5' + CAST(1 AS BIGINT)) AS long_t
        """
      Then query result
        | byte_t | short_t | int_t  | long_t |
        | bigint | bigint  | bigint | bigint |

    Scenario: a string with each integral type, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + CAST(1 AS TINYINT)) AS byte_t,
               typeof('5' + CAST(1 AS SMALLINT)) AS short_t,
               typeof('5' + CAST(1 AS INT)) AS int_t,
               typeof('5' + CAST(1 AS BIGINT)) AS long_t
        """
      Then query result
        | byte_t | short_t | int_t  | long_t |
        | double | double  | double | double |

    Scenario: a string with each fractional type, ANSI on, promotes to double
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof('5' + CAST(1.5 AS FLOAT)) AS float_t,
               typeof('5' + CAST(1.5 AS DOUBLE)) AS double_t,
               typeof('5' + CAST(1.5 AS DECIMAL(10,2))) AS decimal_t
        """
      Then query result
        | float_t | double_t | decimal_t |
        | double  | double   | double    |

    Scenario: a string with each fractional type, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + CAST(1.5 AS FLOAT)) AS float_t,
               typeof('5' + CAST(1.5 AS DOUBLE)) AS double_t,
               typeof('5' + CAST(1.5 AS DECIMAL(10,2))) AS decimal_t
        """
      Then query result
        | float_t | double_t | decimal_t |
        | double  | double   | double    |

  Rule: Literal narrowing over every integer literal type
    # `DataTypeUtils.fromLiteral` matches Short, Int and Long by value; a Byte literal
    # falls through to the type-based decimal(3,0). Only `*` makes the difference visible.

    Scenario: each integer literal type against a decimal
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3Y) AS byte_t,
               typeof(CAST(2.5 AS DECIMAL(10,2)) * 3S) AS short_t,
               typeof(CAST(2.5 AS DECIMAL(10,2)) * 3) AS int_t,
               typeof(CAST(2.5 AS DECIMAL(10,2)) * 3L) AS long_t
        """
      Then query result
        | byte_t        | short_t       | int_t         | long_t        |
        | decimal(14,2) | decimal(12,2) | decimal(12,2) | decimal(12,2) |

  Rule: A fractional string cast to an integer truncates under ANSI off
    # Spark parses with `UTF8String.toInt`, which reads the sign and digits, IGNORES the
    # fractional part (truncating toward zero, never rounding) and rejects exponents —
    # `'1e2'` is NULL even though `1e2` is a valid double. Arrow's parser instead rejects
    # the whole string, so Sail yields NULL where Spark truncates. Under ANSI both raise,
    # so only the ANSI-off half diverges.
    # Note `CAST('1.5' AS DECIMAL(10,0))` is a different rule: it ROUNDS to 2, in both modes.

    Scenario: a fractional string cast to an integer raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('1.5' AS INT) AS r
        """
      Then query error (?i)cannot (be )?cast

    @sail-bug
    Scenario: a fractional string cast to an integer truncates under ANSI off
      # Spark truncates toward zero: 1, 1, -1, -1. Sail returns NULL for all four.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1.5' AS INT) AS a,
               CAST('1.9' AS INT) AS b,
               CAST('-1.5' AS INT) AS c,
               CAST('-1.9' AS INT) AS d
        """
      Then query result
        | a | b | c  | d  |
        | 1 | 1 | -1 | -1 |

    Scenario: a control-character prefix is trimmed like whitespace
      # Spark's trim strips every byte at or below 0x20, not just the usual whitespace —
      # e.g. the file separator 0x1C. Both a literal and a column go through the shared
      # trim, so this covers both paths.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(concat(char(28), '5') AS INT) AS lit,
               CAST(concat(char(9), s) AS INT) AS col
        FROM VALUES ('7') AS t(s)
        """
      Then query result
        | lit | col |
        | 5   | 7   |

    Scenario: an exponent string cast to an integer is NULL under ANSI off
      # Spark's integer parser rejects exponent notation even though the value is integral.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1e2' AS INT) AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: a fractional string cast to a decimal rounds in both ANSI modes
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1.5' AS DECIMAL(10,0)) AS r
        """
      Then query result
        | r |
        | 2 |

  Rule: A string that does not parse is NULL under ANSI off and raises under ANSI on

    Scenario: a malformed string operand, ANSI off, is NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('not-a-number' + 1) AS t,
               'not-a-number' + 1 AS a,
               'not-a-number' * 2 AS b,
               'not-a-number' - 2 AS c
        """
      Then query result
        | t      | a    | b    | c    |
        | double | NULL | NULL | NULL |

    # The cast target is the point: Spark raises for BIGINT, so a string beyond INT
    # range must not fail here. Sail names the Arrow type (`Int64`) rather than
    # Spark's `BIGINT`, so the pattern matches the wording both share.
    Scenario: a malformed string operand, ANSI on, raises
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 'not-a-number' + CAST(1 AS INT) AS r
        """
      Then query error (?i)cannot (be )?cast

    Scenario: an empty string operand, ANSI off, is NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT '' + 1 AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: a surrounding-whitespace string operand, ANSI off, still parses
      # Spark trims before parsing (UTF8String.trim); Arrow does not. `CAST`, the type
      # constructors and this coercion all share `spark_string_to_numeric` so they agree.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ' 5 ' + 1 AS r
        """
      Then query result
        | r   |
        | 6.0 |

  Rule: The string coercions hold for columns, not just literals
    # A literal is trimmed at plan time while a column goes through `btrim`, so these are
    # two distinct code paths and the literal scenarios above do not cover the second one.

    Scenario: a string column in arithmetic, ANSI off, over multiple rows
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(s + 1) AS t, s + 1 AS r
        FROM VALUES (' 5 '), ('7'), ('not-a-number'), (''), (NULL), ('  8  ') AS t(s)
        """
      Then query result
        | t      | r    |
        | double | 6.0  |
        | double | 8.0  |
        | double | NULL |
        | double | NULL |
        | double | NULL |
        | double | 9.0  |

    Scenario: a string column plus an int column, ANSI on, promotes to bigint
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(s + n) AS t, s + n AS r
        FROM VALUES (' 5 ', 1), ('3000000000', 1) AS t(s, n)
        """
      Then query result
        | t      | r          |
        | bigint | 6          |
        | bigint | 3000000001 |

    Scenario: CAST of a string column to a numeric, ANSI off, over multiple rows
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(s AS DOUBLE) AS r
        FROM VALUES (' 5 '), ('not-a-number'), (''), (NULL), ('1e400') AS t(s)
        """
      Then query result
        | r        |
        | 5.0      |
        | NULL     |
        | NULL     |
        | NULL     |
        | Infinity |

    Scenario: the DOUBLE constructor over a string column
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT DOUBLE(s) AS r
        FROM VALUES (' NaN '), (' Infinity '), (' 2.5 ') AS t(s)
        """
      Then query result
        | r        |
        | NaN      |
        | Infinity |
        | 2.5      |

  Rule: A NULL operand takes the other operand's type, except for division
    # Spark coerces NullType to the peer's type, so the decimal rule applies as if the
    # NULL were that decimal. Division is asymmetric: `decimal / NULL` follows the
    # decimal rule, but `NULL / decimal` falls back to double.
    #
    Scenario: decimal divided by NULL follows the decimal division rule
      When query
        """
        SELECT typeof(CAST(3 AS DECIMAL(10,2)) / NULL) AS t,
               CAST(3 AS DECIMAL(10,2)) / NULL AS r
        """
      Then query result
        | t              | r    |
        | decimal(23,13) | NULL |

    Scenario: NULL divided by a decimal falls back to double
      When query
        """
        SELECT typeof(NULL / CAST(3 AS DECIMAL(10,2))) AS t,
               NULL / CAST(3 AS DECIMAL(10,2)) AS r
        """
      Then query result
        | t      | r    |
        | double | NULL |

    Scenario: a NULL operand in multiply is symmetric
      When query
        """
        SELECT typeof(CAST(3 AS DECIMAL(10,2)) * NULL) AS a,
               typeof(NULL * CAST(3 AS DECIMAL(10,2))) AS b
        """
      Then query result
        | a             | b             |
        | decimal(21,4) | decimal(21,4) |

    Scenario: a NULL operand in plus takes the decimal type
      When query
        """
        SELECT typeof(CAST(3 AS DECIMAL(10,2)) + NULL) AS a,
               typeof(NULL + CAST(1 AS INT)) AS b
        """
      Then query result
        | a             | b   |
        | decimal(11,2) | int |

    Scenario: string plus NULL, ANSI off, promotes to double
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof('5' + NULL) AS t, '5' + NULL AS r
        """
      Then query result
        | t      | r    |
        | double | NULL |

    # Both pairings are rejected by Spark and by Sail, so the coercion side already
    # matches — what diverges is the error itself: Spark raises the
    # BINARY_OP_WRONG_TYPE class, while Sail reports the Arrow types (`Utf8 + Utf8`).
    # These assert Spark's error class, so they xfail until Sail grows Spark error
    # classes; the pattern is deliberately the class name rather than a substring both
    # happen to share, which would only assert "some error".

    @sail-bug
    Scenario: string plus NULL, ANSI on, is rejected
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT '5' + NULL AS r
        """
      Then query error BINARY_OP_WRONG_TYPE

    @sail-bug
    Scenario: string plus string, ANSI on, is rejected
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT '5' + '6' AS r
        """
      Then query error BINARY_OP_WRONG_TYPE

  Rule: Decimal precision configs change the result type
    # Spark exposes two switches that change the decimal result type. `allowPrecisionLoss`
    # picks `adjustPrecisionScale` (reduce the scale to keep the integer digits) over
    # `bounded` (clamp both, so an unrepresentable result is NULL); note divide does not
    # simply swap in `bounded` — it keeps Hive's older sizing. `pickMinimumPrecision`
    # decides whether an integer literal narrows to its value's decimal or to its type's.

    Scenario: legacy pickMinimumPrecision=false stops narrowing an integer literal
      Given config spark.sql.legacy.literal.pickMinimumPrecision = false
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) * 3) AS t
        """
      Then query result
        | t             |
        | decimal(21,2) |

    Scenario: allowPrecisionLoss=false keeps the full scale on multiply
      Given config spark.sql.decimalOperations.allowPrecisionLoss = false
      When query
        """
        SELECT typeof(CAST(1.5 AS DECIMAL(38,10)) * CAST(2.5 AS DECIMAL(38,10))) AS t,
               CAST(1.5 AS DECIMAL(38,10)) * CAST(2.5 AS DECIMAL(38,10)) AS r
        """
      Then query result
        | t              | r                    |
        | decimal(38,20) | 3.75000000000000000000 |

    Scenario: allowPrecisionLoss=false keeps the full scale on divide
      Given config spark.sql.decimalOperations.allowPrecisionLoss = false
      When query
        """
        SELECT typeof(CAST(1.5 AS DECIMAL(38,10)) / CAST(2.5 AS DECIMAL(38,10))) AS t
        """
      Then query result
        | t              |
        | decimal(38,18) |

    Scenario: allowPrecisionLoss defaults to true and caps the scale
      Given config spark.sql.decimalOperations.allowPrecisionLoss = true
      When query
        """
        SELECT typeof(CAST(1.5 AS DECIMAL(38,10)) * CAST(2.5 AS DECIMAL(38,10))) AS t,
               CAST(1.5 AS DECIMAL(38,10)) * CAST(2.5 AS DECIMAL(38,10)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 3.750000 |

  Rule: A full-scale decimal(38,38) division overflows the intermediate (known gap)
    # Spark computes decimal division in BigDecimal at scale 39, so its intermediate is
    # unbounded in practice. Sail rewrites the division over a widened i256, but Arrow's
    # decimal `div` rescales the numerator by 10^(4 + s2) before dividing, so the
    # intermediate carries (p1 - s1) + dividend_scale + 4 + s2 digits — 80 here, against
    # i256's ~76 — and the rescale raises. Widening alone does not prevent this.
    #
    # The overflow is value-dependent, not type-dependent: only rows whose numerator
    # actually exceeds i256 raise, so the Spark-scale path is still taken (rejecting the
    # whole type would send every row to the native divide, which overflows i128 even
    # sooner). Closing it needs BigDecimal semantics in the custom PhysicalExpr follow-up:
    # emulating Spark exactly needs 1e38 x 1e39 = 1e77, which does not fit i256 either.

    @sail-bug
    Scenario: decimal(38,38) divided by itself is exactly 1
      When query
        """
        SELECT typeof(CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))
                      / CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))) AS t,
               CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))
               / CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38)) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 1.000000 |

    Scenario: decimal(38,38) multiplied by itself caps to the adjusted scale
      When query
        """
        SELECT typeof(CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))
                      * CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))) AS t,
               CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38))
               * CAST('0.99999999999999999999999999999999999999' AS DECIMAL(38,38)) AS r
        """
      Then query result
        | t              | r                                       |
        | decimal(38,37) | 1.0000000000000000000000000000000000000 |
