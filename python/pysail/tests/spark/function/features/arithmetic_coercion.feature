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
    # The literal -3 narrows to Decimal(1,0) (the sign does not add a digit), so
    # `dec * -3` has the same result type as `dec * 3`. That row guards against a
    # `Negative(Literal)` plan node skipping the narrowing.

    Scenario Outline: Literal narrowing: <case>
      When query
        """
        SELECT typeof(<expr>) AS t,
               <expr> AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                            | expr                             | t             | r      |
        | decimal times a single-digit integer literal    | CAST(2.5 AS DECIMAL(10,2)) * 3   | decimal(12,2) | 7.50   |
        | decimal plus a single-digit integer literal     | CAST(2.5 AS DECIMAL(10,2)) + 3   | decimal(11,2) | 5.50   |
        | decimal minus a single-digit integer literal    | CAST(5.5 AS DECIMAL(10,2)) - 2   | decimal(11,2) | 3.50   |
        | decimal times a three-digit integer literal     | CAST(2.5 AS DECIMAL(10,2)) * 100 | decimal(14,2) | 250.00 |
        | a negative integer literal narrows the same way | CAST(2.5 AS DECIMAL(10,2)) * -3  | decimal(12,2) | -7.50  |

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

    # @spark-4: `interval day` renders as bare `interval` on PySpark 3.5.
    @sail-bug @spark-4
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
    #
    # The `fits in 38` row guards the gate from the other side: below precision 38 Arrow
    # already agrees with Spark, so the re-typing must NOT fire.

    Scenario Outline: Capped type and value: <case>
      When query
        """
        SELECT typeof(<type_expr>) AS t,
               <value_expr> AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                                | type_expr                                               | value_expr                                                 | t             | r        |
        | a wide decimal sum reduces the scale                | CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,2))    | CAST(1.005 AS DECIMAL(38,10)) + CAST(2.5 AS DECIMAL(38,2)) | decimal(38,6) | 3.505000 |
        | a decimal sum that fits in 38 keeps the native type | CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2)) | CAST(2.5 AS DECIMAL(10,2)) + CAST(3.5 AS DECIMAL(10,2))    | decimal(11,2) | 6.00     |

    Scenario Outline: Capped type only: <case>
      When query
        """
        SELECT typeof(<expr>) AS t
        """
      Then query result
        | t   |
        | <t> |

      Examples:
        | case                                                            | expr                                                 | t              |
        | a wide decimal difference reduces the scale to the adjusted one | CAST(1 AS DECIMAL(38,10)) - CAST(1 AS DECIMAL(38,2)) | decimal(38,6)  |
        | a decimal plus an integer literal reduces the scale past 38     | CAST(1 AS DECIMAL(38,18)) + 2                        | decimal(38,17) |

    Scenario: a wide decimal sum over columns rounds the value per row
      # The literal cases fold at plan time; this exercises the round+cast over a real
      # batch, so the HALF_UP rounding to the reduced scale is proven at runtime.
      When query
        """
        SELECT typeof(a + b) AS t, a + b AS r
        FROM VALUES
          (CAST(1.005 AS DECIMAL(38,10)), CAST(2.5 AS DECIMAL(38,2))),
          (CAST(-1.005 AS DECIMAL(38,10)), CAST(2.5 AS DECIMAL(38,2))),
          (CAST(1.0000005 AS DECIMAL(38,10)), CAST(0 AS DECIMAL(38,2))),
          (CAST(NULL AS DECIMAL(38,10)), CAST(2.5 AS DECIMAL(38,2)))
          AS t(a, b)
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 3.505000 |
        | decimal(38,6) | 1.495000 |
        | decimal(38,6) | 1.000001 |
        | decimal(38,6) | NULL     |

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

    Scenario Outline: String operand ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expr>) AS t, <expr> AS r
        """
      Then query result
        | t      | r   |
        | double | <r> |

      Examples:
        | case                                   | expr      | r   |
        | string plus integer promotes to double | '5' + 3   | 8.0 |
        | string plus string promotes to double  | '5' + '3' | 8.0 |

    Scenario Outline: String operand with a fractional peer: <case>
      When query
        """
        SELECT typeof(<expr>) AS t,
               <expr> AS r
        """
      Then query result
        | t      | r   |
        | double | <r> |

      Examples:
        | case                              | expr                             | r   |
        | string plus decimal-typed literal | '5' + 3.5                        | 8.5 |
        | string plus decimal column        | '5' + CAST(2.5 AS DECIMAL(10,2)) | 7.5 |

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

    Scenario Outline: Modulo type: <case>
      When query
        """
        SELECT typeof(<expr>) AS t,
               <expr> AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                       | expr                                                       | t             | r     |
        | decimal modulo decimal                     | CAST(10.5 AS DECIMAL(10,2)) % CAST(3.0 AS DECIMAL(10,2))   | decimal(10,2) | 1.50  |
        | decimal modulo decimal, different scales   | CAST(10.567 AS DECIMAL(10,3)) % CAST(3.1 AS DECIMAL(10,1)) | decimal(10,3) | 1.267 |
        | double modulo a decimal promotes to double | CAST(1.5 AS DOUBLE) % CAST(2.0 AS DECIMAL(10,2))           | double        | 1.5   |

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
        | t              | r               |
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
        | t              | r             |
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
    # is not collapsed. That is the point of these scenarios: division + simplify are
    # correct. Only the VALUE is asserted; `round`'s own decimal TYPE still follows
    # DataFusion's rule, not Spark's `RoundBase.dataType`, which is a pre-existing gap
    # outside this change.

    Scenario: round of a decimal division to a smaller scale
      When query
        """
        SELECT round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 2) AS r
        """
      Then query result
        | r    |
        | 3.33 |

    Scenario: round of a division rounds HALF_UP after dividing
      When query
        """
        SELECT round(CAST(2.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 4) AS r
        """
      Then query result
        | r      |
        | 0.6667 |

    Scenario: round of a negative decimal division
      When query
        """
        SELECT round(CAST(-5.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 6) AS r
        """
      Then query result
        | r         |
        | -1.666667 |

    Scenario: round of a decimal division to zero scale
      When query
        """
        SELECT round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 0) AS r
        """
      Then query result
        | r |
        | 3 |

    Scenario: a capped sum that overflows only at the native scale keeps Spark's value
      # A native i128 add at the bounded scale 10 would overflow (1.75e28 needs more than i128
      # holds), but Spark's adjusted type decimal(38,6) represents the sum — so the `+`/`-`
      # retype path widens to Decimal256 before adding (like the capped multiply) and both
      # engines return the VALUE.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(8750000000000000000000000000 AS DECIMAL(38,10))
                      + CAST(8750000000000000000000000000 AS DECIMAL(38,2))) AS t,
               CAST(8750000000000000000000000000 AS DECIMAL(38,10))
               + CAST(8750000000000000000000000000 AS DECIMAL(38,2)) AS r
        """
      Then query result
        | t             | r                                    |
        | decimal(38,6) | 17500000000000000000000000000.000000 |

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

    # Both engines raise, so the behaviour agrees; only the error class diverges. Spark's
    # message is "... cannot be represented as Decimal(38, 0)" (class NUMERIC_VALUE_OUT_OF_RANGE),
    # which does not contain the word "overflow" — Sail reports its own Arrow overflow error.
    # We assert Spark's class so this xfails on Sail; a lax `(?i)overflow` pattern would
    # instead pass against Spark for the wrong reason (the word appears only in the JVM
    # stack trace, not in the error message).
    @sail-bug
    Scenario: a capped decimal sum that overflows raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9e37 AS DECIMAL(38,0)) + CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

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

    @sail-bug
    Scenario: a capped decimal difference that overflows raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(-9e37 AS DECIMAL(38,0)) - CAST(9e37 AS DECIMAL(38,0)) AS r
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

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

  @function(nullability)
  Rule: Decimal arithmetic is nullable in Spark (known gap — needs custom PhysicalExpr)
    # Spark marks decimal +, -, * as nullable=true even for non-null operands,
    # because the operation can overflow to NULL. A native BinaryExpr built in the
    # plan builder inherits nullability from its operands, so Sail reports false.
    #
    # Sail therefore agrees with Spark only where the operator is re-typed through a
    # `try_cast` (DataFusion types that nullable=true unconditionally): the capped
    # multiply, the capped `+`/`-`, the decimal divide and the remainder, all under ANSI
    # off. Both sides of that split are pinned below, so the custom PhysicalExpr follow-up
    # — which makes every one of them nullable=true — cannot silently regress the half
    # that already agrees.
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

    # `decimal(38,0) + decimal(38,0)` has exact precision 39, yet both `cap` and `bounded`
    # collapse to (38,0), so `spark_decimal_add_diverges` is false and the sum stays a native
    # add reporting nullable=false — unlike `*`, whose gate fires on `p1+p2+1 > 38` regardless
    # of type equality. Same custom-PhysicalExpr follow-up; pinned so this type-unchanged `+`/`-`
    # shape is not mistaken for covered.
    @sail-bug
    Scenario: a wide decimal sum whose type is unchanged is still nullable like Spark
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0)) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(38,0) (nullable = true)
        """

    Scenario: a capped decimal multiply is already nullable like Spark
      # p1+p2+1 > 38, so the product is narrowed with `try_cast` and the declared
      # nullability matches Spark without the follow-up.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,18)) * CAST(1 AS DECIMAL(38,18)) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(38,6) (nullable = true)
        """

    Scenario: a capped decimal sum is nullable like Spark
      # p1-s1/p2-s2 push the exact precision past 38, so the retype narrows with
      # `try_cast` under ANSI off (like the capped multiply above) and the declared
      # nullability matches Spark without the custom-PhysicalExpr follow-up.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,2)) AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(38,6) (nullable = true)
        """

    Scenario: a capped decimal multiply and sum are non-nullable under ANSI on
      # The complement of the two ANSI-off scenarios above: under ANSI on the retype narrows
      # with a strict `cast`, so `nullOnOverflow = false` and the result is nullable=false,
      # matching Spark's CheckOverflow. Pinned so the custom-PhysicalExpr follow-up cannot
      # blindly force nullable=true across both modes and regress the ANSI-on half undetected.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(1 AS DECIMAL(38,18)) * CAST(1 AS DECIMAL(38,18)) AS m,
               CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,2)) AS s
        """
      Then query schema
        """
        root
         |-- m: decimal(38,6) (nullable = false)
         |-- s: decimal(38,6) (nullable = false)
        """

    Scenario: division, remainder and pmod are nullable under ANSI on
      # Spark's DivModLike.nullable and Pmod.nullable are unconditionally true,
      # not ANSI-dependent. Sail already agrees; pinned so the custom PhysicalExpr
      # follow-up cannot regress the half that matches.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(10 AS INT) / CAST(2 AS INT) AS d,
               CAST(10.5 AS DECIMAL(10,2)) % CAST(3 AS INT) AS m,
               pmod(CAST(10 AS INT), CAST(3 AS INT)) AS p
        """
      Then query schema
        """
        root
         |-- d: double (nullable = true)
         |-- m: decimal(10,2) (nullable = true)
         |-- p: integer (nullable = true)
        """

  Rule: try_add shares the + operator's decimal typing (known gap)
    # Spark evaluates try_* through the same resultDecimalType as the operators
    # (EvalMode.TRY), so try_add(decimal(38,10), decimal(38,2)) is decimal(38,6).
    # Sail's spark_try_add UDF rejects the pair outright ("expects Int32, Int64,
    # Date32 or Interval"), so it neither types nor computes what Spark does.

    @sail-bug
    Scenario: try_add of wide decimals takes the operator's capped type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(try_add(CAST(1 AS DECIMAL(38,10)), CAST(1 AS DECIMAL(38,2)))) AS t,
               try_add(CAST(1 AS DECIMAL(38,10)), CAST(1 AS DECIMAL(38,2))) AS r
        """
      Then query result
        | t             | r        |
        | decimal(38,6) | 2.000000 |

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

  Rule: ANSI string promotion and pmod follow the operators' coercion
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

    # `pmod` is a UDF (SparkPmod) but takes the same operand coercion as the
    # operators, which fixes the float x decimal promotion; the plan builder also
    # gives a bare NULL its peer's type and narrows the result to Spark's remainder
    # rule (`min(p1-s1, p2-s2) + max(s1,s2)`). Follow-up: move that rule and the
    # NULL handling into `SparkPmod` itself, whose own return type is still un-Spark
    # and only patched over by the outer cast.
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
      # Spark returns NULL. `SparkPmod`'s `Signature::numeric` seeds coercion from the
      # first argument, so the plan builder gives a bare NULL its peer's type first.
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

    Scenario: pmod remainder-type overflow over columns, ANSI off
      # The literal forms fold at plan time; this drives the ANSI-off try_cast narrowing
      # over a batch. A row that fits keeps its value, the overflowing row is NULL.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT pmod(a, b) AS r
        FROM VALUES
          (CAST(-5.00 AS DECIMAL(3,2)), CAST(99999 AS DECIMAL(5,0))),
          (CAST(1.50 AS DECIMAL(3,2)), CAST(2 AS DECIMAL(5,0)))
          AS t(a, b)
        """
      Then query result
        | r    |
        | NULL |
        | 1.50 |

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

  Rule: A BYTE literal is not narrowed — Spark narrows only Short, Int and Long
    # `DataTypeUtils.fromLiteral` narrows Short/Int/Long literals to their minimal
    # decimal, but a Byte literal falls through to `forType(ByteType)` = decimal(3,0).
    # For the `decimal(10,2)` shapes below the difference shows up only in `*` (whose
    # precision depends on the operand's precision); `+`/`/` happen to coincide here,
    # though they can differ for other shapes.

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

    Scenario: a byte literal gives the same plus and divide types as an int literal here
      # Not because the byte narrows — it does not — but because for these decimal(10,2)
      # shapes the type-based decimal(3,0) and the narrowed decimal happen to coincide.
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

  Rule: A fractional string cast to an integer truncates under ANSI off, raises under ANSI on
    # Spark parses with `UTF8String.toInt`, which reads the sign and digits, IGNORES the
    # fractional part (truncating toward zero, never rounding) and rejects exponents.
    # Arrow's parser instead rejects the whole string, so Sail yields NULL under ANSI off
    # where Spark truncates, and raises a different error class under ANSI on.

    @sail-bug
    Scenario: a fractional string cast to an integer raises under ANSI on
      # Both raise; Spark names the CAST_INVALID_INPUT class, Sail reports the Arrow cast
      # error ("Cannot cast string '1.5' to value of Int32 type"). Asserted as Spark's
      # class so it xfails until Sail grows Spark error classes, not as a shared substring.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('1.5' AS INT) AS r
        """
      Then query error CAST_INVALID_INPUT

    Scenario: a fractional string cast to an integer truncates under ANSI off
      # Spark truncates toward zero: 1, 1, -1, -1; Sail matches.
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

  Rule: An exponent string cast to an integer is rejected even though the value is integral
    # Spark's integer parser rejects exponent notation: NULL under ANSI off, raises under
    # ANSI on. Sail agrees on the ANSI-off NULL.

    Scenario: an exponent string cast to an integer is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1e2' AS INT) AS r
        """
      Then query result
        | r    |
        | NULL |

    @sail-bug
    Scenario: an exponent string cast to an integer raises under ANSI on
      # Spark: CAST_INVALID_INPUT; Sail reports the Arrow cast error.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('1e2' AS INT) AS r
        """
      Then query error CAST_INVALID_INPUT

  Rule: A control character at or below 0x20 is trimmed like whitespace
    # Spark's trim strips every byte at or below 0x20, not just the usual whitespace — the
    # file separator 0x1C and even the NUL 0x00. The literal and the column go through
    # different trims (plan-time vs `btrim`), so both are exercised, and both ANSI modes
    # agree because the trimmed value parses cleanly.

    Scenario: control characters are trimmed under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(concat(char(28), '5') AS INT) AS lit,
               CAST(concat(char(28), s) AS INT) AS fs_col,
               CAST(concat(char(0), s) AS INT) AS nul_col
        FROM VALUES ('7') AS t(s)
        """
      Then query result
        | lit | fs_col | nul_col |
        | 5   | 7      | 7       |

    Scenario: control characters are trimmed under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(concat(char(28), '5') AS INT) AS r
        """
      Then query result
        | r |
        | 5 |

    Scenario: the DEL control character is trimmed by a cast to integer
      # Spark's integer parser trims a slightly wider set than 0x20 — DEL (0x7F) among the
      # ISO controls — so Spark returns 5; Sail matches (Arrow's integer parser ignores the
      # trailing DEL past the digits).
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(concat('5', char(127)) AS INT) AS r
        """
      Then query result
        | r |
        | 5 |

  Rule: A fractional string cast to a decimal rounds, in both ANSI modes
    Scenario: a fractional string cast to a decimal rounds under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1.5' AS DECIMAL(10,0)) AS r
        """
      Then query result
        | r |
        | 2 |

    Scenario: a fractional string cast to a decimal rounds under ANSI on
      Given config spark.sql.ansi.enabled = true
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
      # Spark trims before parsing, Arrow does not. `CAST`, the type
      # constructors and this coercion all share `spark_string_to_numeric` so they agree.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT ' 5 ' + 1 AS r
        """
      Then query result
        | r   |
        | 6.0 |

  Rule: A Java-suffixed or hexadecimal float string parses like Spark (known gap)
    # Spark's string-to-double cast is Java's `Double.parseDouble` (Cast.scala), whose
    # grammar also accepts a trailing `f`/`F`/`d`/`D` type suffix and hexadecimal
    # floats such as `0x1.8p1`. Arrow's parser rejects all of these, so Sail yields
    # NULL under ANSI off where Spark parses the value.

    @sail-bug
    Scenario: a float-suffixed string operand parses like Java
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT '1.5f' + 1 AS r
        """
      Then query result
        | r   |
        | 2.5 |

    @sail-bug
    Scenario: a double-suffixed string cast to DOUBLE parses like Java
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('2d' AS DOUBLE) AS r
        """
      Then query result
        | r   |
        | 2.0 |

    @sail-bug
    Scenario: a hexadecimal float string cast to DOUBLE parses like Java
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('0x1.8p1' AS DOUBLE) AS r
        """
      Then query result
        | r   |
        | 3.0 |

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
        | t              | r                      |
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

  Rule: Decimal division stays on the cheaper i128 kernel when the intermediate fits
    # The `/` decimal path gates its i256 widening: when the rescaled numerator provably fits
    # 38 digits it computes the quotient in i128 (2-4x cheaper per row), only widening to i256
    # for the wide cases (the D1 gap below). Both are value-identical to Spark's BigDecimal
    # HALF_UP; these pin the i128 fast-path across the boundary so a broken gate (an i128
    # overflow) fails a test — the `two thirds` row also discriminates HALF_UP from half-even
    # (0.666...667, not ...666). Verified against Spark 4.2.0.

    Scenario Outline: i128 decimal division: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expr>) AS t, CAST((<expr>) AS STRING) AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                         | expr                                                                             | t              | r                    |
        | one third rounds HALF_UP     | CAST(1 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2))                               | decimal(23,13) | 0.3333333333333      |
        | two thirds rounds HALF_UP    | CAST(2 AS DECIMAL(10,2)) / CAST(3 AS DECIMAL(10,2))                               | decimal(23,13) | 0.6666666666667      |
        | a narrow ratio               | CAST('12345.67' AS DECIMAL(10,2)) / CAST('3.14' AS DECIMAL(10,2))                 | decimal(23,13) | 3931.7420382165605   |
        | a scale-10 self-division     | CAST('55.1234567891' AS DECIMAL(20,10)) / CAST('55.1234567891' AS DECIMAL(20,10)) | decimal(38,18) | 1.000000000000000000 |
        | a near-boundary division     | CAST('7.123456789012345678' AS DECIMAL(38,18)) / CAST('3.5' AS DECIMAL(38,18))    | decimal(38,6)  | 2.035273             |
        | a wide dividend narrow ratio | CAST('99.1234567891' AS DECIMAL(38,10)) / CAST('7' AS DECIMAL(38,10))             | decimal(38,6)  | 14.160494            |

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

    # The exposure is exactly two types wide. Scale 36 and below divide fine; the
    # intermediate only exceeds i256 at scale 37 and 38, which no schema produces on its
    # own — they need an explicit CAST. These three scenarios pin that frontier so a
    # future fix (or a regression) is visible at the boundary rather than at one point.

    Scenario: a scale-36 decimal divided by itself stays within i256
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,36)) / CAST('0.5' AS DECIMAL(38,36)) AS r
        """
      Then query result
        | r        |
        | 1.000000 |

    @sail-bug
    Scenario: a scale-37 decimal divided by itself overflows the intermediate
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,37)) / CAST('0.5' AS DECIMAL(38,37)) AS r
        """
      Then query result
        | r        |
        | 1.000000 |

    @sail-bug
    Scenario: a scale-38 division of different values overflows the same way
      # Not a self-division artifact: any pair at scale 38 hits it.
      When query
        """
        SELECT CAST('0.1' AS DECIMAL(38,38)) / CAST('0.5' AS DECIMAL(38,38)) AS r
        """
      Then query result
        | r        |
        | 0.200000 |

    # A 78-case grid over (precision, scale), literal vs column and both ANSI modes gives
    # the exact shape: it takes precision 38 AND BOTH scales at 37 or above. One high
    # scale is not enough, and a narrower precision at full scale is fine. The scenarios
    # below pin both sides of that shape — the ones that must keep working as much as the
    # ones that fail.

    Scenario: full-scale division at a narrower precision works
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(20,20)) / CAST('0.5' AS DECIMAL(20,20)) AS narrow,
               CAST('0.5' AS DECIMAL(30,30)) / CAST('0.5' AS DECIMAL(30,30)) AS mid
        """
      Then query result
        | narrow               | mid        |
        | 1.000000000000000000 | 1.00000000 |

    Scenario: one high scale against a low one works
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,38)) / CAST('0.5' AS DECIMAL(38,10)) AS r
        """
      Then query result
        | r                              |
        | 1.0000000000000000000000000000 |

    @sail-bug
    Scenario: mixed scales 38 and 37 overflow in either order
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,38)) / CAST('0.5' AS DECIMAL(38,37)) AS r
        """
      Then query result
        | r        |
        | 1.000000 |

    @sail-bug
    Scenario: the overflow is not a constant-folding artifact — it hits columns too
      # The literal scenarios are folded while the plan is built, so this one drives the
      # same division through the runtime kernel.
      When query
        """
        SELECT x / y AS r
        FROM VALUES (CAST('0.5' AS DECIMAL(38,38)), CAST('0.5' AS DECIMAL(38,38))) AS t(x, y)
        """
      Then query result
        | r        |
        | 1.000000 |

    @sail-bug
    Scenario: the overflow ignores ANSI mode
      # Spark returns the value in both modes; ANSI off does not turn it into NULL either.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,38)) / CAST('0.5' AS DECIMAL(38,38)) AS r
        """
      Then query result
        | r        |
        | 1.000000 |

    Scenario: the other operators are unaffected at full scale
      # Only division rewrites through i256; `*`, `%` and `+` stay on their own paths.
      When query
        """
        SELECT CAST('0.5' AS DECIMAL(38,38)) % CAST('0.5' AS DECIMAL(38,38)) AS modulo,
               typeof(CAST('0.5' AS DECIMAL(38,38)) + CAST('0.5' AS DECIMAL(38,38))) AS sum_type
        """
      Then query result
        | modulo                                   | sum_type       |
        | 0.00000000000000000000000000000000000000 | decimal(38,37) |

    @sail-bug
    Scenario: modulo of mixed extreme scales overflows the native kernel
      # Equal scales need no rescale (the scenario above), but a scale-0 dividend
      # against a scale-38 divisor is rescaled by 10^38 in i128 inside Arrow's `rem`,
      # which overflows for any non-zero dividend. Spark computes in BigDecimal and
      # returns the value — the same native-kernel overflow family as the wide sum,
      # deferred to the custom PhysicalExpr follow-up.
      When query
        """
        SELECT CAST(7 AS DECIMAL(38,0)) % CAST(0.3 AS DECIMAL(38,38)) AS r
        """
      Then query result
        | r                                        |
        | 0.10000000000000000000000000000000000000 |

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

  Rule: Type derivation holds over VALUES inference, columns, foldable scales and NULL peers
    # Each scenario below reproduces a divergence confirmed against Spark 4.1.1. All but the
    # last are closed; the remaining `@sail-bug` belongs to the deferred overflow work.

    Scenario: a heterogeneous VALUES list with NaN infers double
      # Under ANSI off the string-to-numeric coercion emits a `TryCast`, so the NaN literal
      # reaches `resolver/query/values.rs` wrapped in `Expr::TryCast` rather than `Expr::Cast`.
      # `cast_parts` recognises both, so VALUES still infers DOUBLE like Spark instead of
      # falling back to decimal(14,7) and failing on NaN.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT b FROM VALUES (0, FLOAT('NaN'), NULL), (1, NULL, 2.0), (2, 2.1, 3.5) t(a, b, c)
        """
      Then query result
        | b    |
        | NaN  |
        | NULL |
        | 2.1  |

    Scenario: TRY_CAST of 'NaN' to an integer in VALUES keeps the integer type
      # The NaN override in `resolver/query/values.rs` must fire only for floating
      # targets: `'NaN'` under an INT target is plain NULL in Spark, so the column
      # stays `int` instead of being widened to float.
      When query
        """
        SELECT typeof(col1) AS t, col1 AS r
        FROM VALUES (TRY_CAST('NaN' AS INT)), (CAST(5 AS INT))
        """
      Then query result
        | t   | r    |
        | int | NULL |
        | int | 5    |

    Scenario: 'NaN' cast to a decimal in VALUES keeps the decimal type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(col1) AS t, col1 AS r
        FROM VALUES (CAST('NaN' AS DECIMAL(10,2)))
        """
      Then query result
        | t             | r    |
        | decimal(10,2) | NULL |

    Scenario: decimal plus an integer column reduces the scale like Spark
      # `+`/`-` apply `coerce_decimal_peer_operand` before the add/sub rule, exactly as
      # `*` and `/` do, so an integer column first takes its type-based decimal
      # (INT -> decimal(10,0)) and `adjustPrecisionScale(39, 18)` then reduces the scale.
      When query
        """
        SELECT typeof(a + b) AS t
        FROM VALUES (CAST(1 AS DECIMAL(38,18)), CAST(1 AS INT)) AS t(a, b)
        """
      Then query result
        | t              |
        | decimal(38,17) |

    Scenario: decimal times NULL takes Spark's capped type
      # Spark converts the NullType operand to the peer's decimal before applying the
      # multiply formula. `coerce_decimal_peer_operand` does the same, so the result is
      # `adjustPrecisionScale(77, 18)` = decimal(38,6), not DataFusion's decimal(38,36).
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,18)) * NULL) AS t
        """
      Then query result
        | t             |
        | decimal(38,6) |

    Scenario: a wide decimal sum computes without an intermediate overflow
      # A native i128 add at the wider scale would overflow before the scale reduction; the
      # `+`/`-` retype path widens to Decimal256 first (like the capped multiply), matching
      # Spark's BigDecimal result.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1000000000000000000000000000000' AS DECIMAL(38,2))
               + CAST(0 AS DECIMAL(38,10)) AS r
        """
      Then query result
        | r                                      |
        | 1000000000000000000000000000000.000000 |

  Rule: Date and timestamp operands follow Spark's temporal arithmetic (known gaps)
    # From the 17x17 operator type-matrix sweep against Spark JVM 4.2 (2026-08-06).
    # Sail types DATE +/- day-time interval as DATE (dropping the time part) and maps
    # DATE - DATE to BIGINT instead of interval day. DATE - TIMESTAMP now types correctly
    # as a day-time interval (below), but diverges under a non-UTC session timezone.

    # The timestamp-typed rows render identically on 3.5 and 4.x, so they stay ungated.
    @sail-bug
    Scenario Outline: Temporal arithmetic: <case>
      When query
        """
        SELECT typeof(<expr>) AS t, CAST(<expr> AS STRING) AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                          | expr                                                   | t         | r                   |
        | date plus a day-time interval is a timestamp  | DATE'2024-01-15' + INTERVAL '1 02:03:04' DAY TO SECOND | timestamp | 2024-01-16 02:03:04 |
        | date minus a day-time interval keeps the time | DATE'2024-01-15' - INTERVAL '1 02:03:04' DAY TO SECOND | timestamp | 2024-01-13 21:56:56 |
        | date plus NULL is a timestamp NULL            | DATE'2024-01-15' + NULL                                | timestamp | NULL                |

    # @spark-4: `interval day to second` renders as bare `interval` on PySpark 3.5, so
    # this interval-typed row only holds on the 4.x oracle. The `-` plan builder casts
    # DATE - TIMESTAMP to a day-time interval, matching Spark.
    @spark-4
    Scenario: a date minus a timestamp is a day-time interval
      When query
        """
        SELECT typeof(DATE'2024-01-15' - TIMESTAMP'2024-01-14 06:30:00') AS t,
               CAST(DATE'2024-01-15' - TIMESTAMP'2024-01-14 06:30:00' AS STRING) AS r
        """
      Then query result
        | t                      | r                                   |
        | interval day to second | INTERVAL '0 17:30:00' DAY TO SECOND |

    # Correct under UTC (above), but under a non-UTC session timezone the value diverges:
    # Spark casts the DATE to TIMESTAMP in the session tz before subtracting
    # (SubtractTimestamps), while Sail/DataFusion computes DATE - TIMESTAMP in UTC, so the
    # result is off by the session offset (here Spark 04:00:00 vs Sail -01:00:00). tz-aware
    # temporal arithmetic is the interval workstream. @spark-4 for the interval rendering.
    @sail-bug @spark-4
    Scenario: a date minus a timestamp under a non-UTC session timezone
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT CAST(DATE'2020-01-02' - TIMESTAMP'2020-01-01 20:00:00' AS STRING) AS r
        """
      Then query result
        | r                                   |
        | INTERVAL '0 04:00:00' DAY TO SECOND |

    # @spark-4: `interval day` renders as bare `interval` on PySpark 3.5. Sail still types
    # DATE - DATE as BIGINT instead of an interval day (interval workstream — Arrow keeps
    # no start/end field, so the result cannot render as `interval day`), a known gap.
    @sail-bug @spark-4
    Scenario: a date minus a date is a day interval
      When query
        """
        SELECT typeof(DATE'2024-03-01' - DATE'2024-01-15') AS t,
               CAST(DATE'2024-03-01' - DATE'2024-01-15' AS STRING) AS r
        """
      Then query result
        | t            | r                 |
        | interval day | INTERVAL '46' DAY |

  Rule: Untyped NULL on both sides types as double (known gap)
    # Spark resolves NULL op NULL as double for all four operators; Sail types
    # +, - and * as bigint (division already agrees).

    # @spark-4: untyped `NULL op NULL` resolves to double only on 4.x (the same gate
    # the sibling null_literal_inference.feature uses for the untyped-NULL family).
    @sail-bug @spark-4
    Scenario: NULL against NULL is double for every operator
      When query
        """
        SELECT typeof(NULL + NULL) AS a, typeof(NULL - NULL) AS b,
               typeof(NULL * NULL) AS c, typeof(NULL / NULL) AS d
        """
      Then query result
        | a      | b      | c      | d      |
        | double | double | double | double |

  Rule: A numeric operand with an untyped NULL keeps the numeric result type
    # An untyped NULL literal (`DataType::Null`, distinct from `CAST(NULL AS INT)` which is a
    # typed INT and stays numeric×numeric) casts to the numeric operand: +, - and * keep the
    # numeric type, / promotes to DOUBLE (Divide's Double input type), and the value is always
    # NULL. Verified identical to Spark 4.2.0 under ANSI on and off.

    Scenario Outline: numeric with untyped NULL, ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expr>) AS t, (<expr>) AS r
        """
      Then query result
        | t   | r    |
        | <t> | NULL |

      Examples:
        | case                    | expr                  | t      |
        | integer plus NULL       | CAST(6 AS INT) + NULL | int    |
        | NULL plus integer       | NULL + CAST(6 AS INT) | int    |
        | integer minus NULL      | CAST(6 AS INT) - NULL | int    |
        | integer times NULL      | CAST(6 AS INT) * NULL | int    |
        | integer divided by NULL | CAST(6 AS INT) / NULL | double |
        | NULL divided by integer | NULL / CAST(6 AS INT) | double |

    Scenario Outline: numeric with untyped NULL, ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(<expr>) AS t, (<expr>) AS r
        """
      Then query result
        | t   | r    |
        | <t> | NULL |

      Examples:
        | case                    | expr                  | t      |
        | integer plus NULL       | CAST(6 AS INT) + NULL | int    |
        | NULL plus integer       | NULL + CAST(6 AS INT) | int    |
        | integer minus NULL      | CAST(6 AS INT) - NULL | int    |
        | integer times NULL      | CAST(6 AS INT) * NULL | int    |
        | integer divided by NULL | CAST(6 AS INT) / NULL | double |
        | NULL divided by integer | NULL / CAST(6 AS INT) | double |

  Rule: A day-time interval times an untyped NULL is a typed-NULL interval
    # Spark's `MultiplyDTInterval` is `ImplicitCastInputTypes`, so an untyped NULL factor
    # casts to the numeric side and `interval * NULL` is a typed-NULL day-time interval, not a
    # rejection (both operand orders). Year-month interval scaling is still unimplemented, so
    # `INTERVAL '1' YEAR * NULL` remains the tracked interval-scaling gap, not this case.

    Scenario: a day-time interval times an untyped NULL keeps the interval type
      When query
        """
        SELECT typeof(INTERVAL '2' DAY * NULL) AS t, (INTERVAL '2' DAY * NULL) AS r1,
               (NULL * INTERVAL '2' DAY) AS r2
        """
      Then query result
        | t                      | r1   | r2   |
        | interval day to second | NULL | NULL |

  Rule: A string divided by an untyped NULL follows ANSI mode
    # Like `string / string`, `string / NULL` has no numeric operand to anchor the implicit
    # cast: ANSI off casts both to DOUBLE (NULL value), ANSI on rejects at analysis. Both
    # operand orders behave the same. Verified against Spark 4.2.0.

    Scenario Outline: string over untyped NULL, ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expr>) AS t, (<expr>) AS r
        """
      Then query result
        | t      | r    |
        | double | NULL |

      Examples:
        | case             | expr       |
        | string over NULL | '6' / NULL |
        | NULL over string | NULL / '6' |

    Scenario Outline: string over untyped NULL, ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r
        """
      Then query error (?i)cannot resolve

      Examples:
        | case             | expr       |
        | string over NULL | '6' / NULL |
        | NULL over string | NULL / '6' |

  Rule: Temporal and interval operands with an untyped NULL (known gaps)
    # From the untyped-NULL sweep vs Spark 4.2.0. An untyped NULL against a DATE/TIMESTAMP is
    # cast to a day-time interval (Spark's `_: DatetimeType, _: NullType` rule), so `date +
    # NULL` is a TIMESTAMP and `date - NULL` a day-time interval; Sail does not coerce the NULL
    # and errors or returns BIGINT. And an interval plus/minus a NULL keeps the operand's
    # narrower field qualifier in Spark (`interval day` / `interval month`) where Sail always
    # widens to `interval day to second` / `interval year to month` (Arrow carries no interval
    # start/end field). Year-month interval scaling by NULL is still unimplemented.
    @sail-bug
    Scenario Outline: untyped NULL against a temporal or interval operand: <case>
      When query
        """
        SELECT typeof(<expr>) AS t
        """
      Then query result
        | t   |
        | <t> |

      Examples:
        | case                                  | expr                     | t                      |
        | date plus untyped NULL is a timestamp | DATE'2024-01-15' + NULL  | timestamp              |
        | date minus untyped NULL is an interval| DATE'2024-01-15' - NULL  | interval day           |
        | timestamp plus untyped NULL           | TIMESTAMP'2024-01-15 12:00:00' + NULL | timestamp |
        | day-time interval plus untyped NULL   | INTERVAL '2' DAY + NULL  | interval day           |
        | year-month interval plus untyped NULL | INTERVAL '2' MONTH + NULL | interval month        |
        | year-month interval times untyped NULL| INTERVAL '2' MONTH * NULL | interval year to month|

  Rule: A DATE offset accepts any integral that fits an INT (including SMALLINT)
    # Spark's DateAdd/DateSub take an INT (IntegerType | ShortType | ByteType), so SMALLINT and
    # TINYINT widen to it and are accepted, while BIGINT/FLOAT/DECIMAL are rejected (the reject
    # half is pinned below). This pins the SMALLINT/TINYINT accept arm of the offset guard so
    # narrowing it — which would wrongly reject `date + smallint` — fails a test.

    Scenario: a date plus or minus a smallint or tinyint stays a date
      When query
        """
        SELECT typeof(DATE'2024-01-15' + CAST(2 AS SMALLINT)) AS a,
               CAST(DATE'2024-01-15' + CAST(2 AS SMALLINT) AS STRING) AS av,
               CAST(DATE'2024-01-15' - CAST(2 AS SMALLINT) AS STRING) AS bv,
               CAST(DATE'2024-01-15' + CAST(3 AS TINYINT) AS STRING) AS cv
        """
      Then query result
        | a    | av         | bv         | cv         |
        | date | 2024-01-17 | 2024-01-13 | 2024-01-18 |

  Rule: Operand pairs Spark rejects at analysis are rejected at plan time
    # Spark raises DATATYPE_MISMATCH at analysis for these pairs (a boolean or timestamp
    # in a division, a DATE/TIMESTAMP offset wider than INT, a bare number combined with
    # an interval, a year-month interval mixed with a day-time one, an interval multiplied
    # by a non-numeric). Left to DataFusion these compute a nonsense value (true / 3 =
    # 0.333..., TIMESTAMP / 3 = raw microseconds, DATE + BIGINT truncated by date_add,
    # year-month + day-time as a CalendarInterval), so Sail's arithmetic plan builders
    # reject them at plan time — matching Spark's accept/reject decision. Spark rejects the
    # same pairs at analysis with DATATYPE_MISMATCH, whose message ("Cannot resolve ... due
    # to data type mismatch") shares the "cannot resolve" prefix with Sail's own "cannot
    # resolve arithmetic" message, so the assertion is oracle-portable across both engines
    # (verified against JVM Spark 4.2.0). Sail's exact error class is a separate systemic gap.
    Scenario Outline: Rejected operand pair: <case>
      When query
        """
        SELECT <expr> AS r
        """
      Then query error (?i)cannot resolve

      Examples:
        | case                                          | expr                                                               |
        | a boolean divided by an integer               | true / CAST(3 AS INT)                                              |
        | a timestamp divided by an integer             | TIMESTAMP'2024-01-15 12:00:00' / CAST(3 AS INT)                    |
        | an integer divided by a day-time interval     | CAST(3 AS INT) / INTERVAL '1 02:03:04' DAY TO SECOND               |
        | a date plus a bigint                          | DATE'2024-01-15' + CAST(3 AS BIGINT)                               |
        | a date minus a bigint                         | DATE'2024-01-15' - CAST(3 AS BIGINT)                              |
        | an integer plus a year-month interval         | CAST(3 AS INT) + INTERVAL '2' MONTH                                |
        | a year-month plus a day-time interval         | INTERVAL '1-2' YEAR TO MONTH + INTERVAL '1 02:03:04' DAY TO SECOND |
        | a date times a day-time interval              | DATE'2024-01-15' * INTERVAL '2' DAY                                |
        | a year-month interval times a day-time one    | INTERVAL '1-2' YEAR TO MONTH * INTERVAL '2' DAY                    |

  Rule: An interval scaled by an integer (known gap — interval arithmetic)
    # Spark multiplies and divides year-month intervals by numerics; Sail rejects
    # every IYM x numeric pair ("Cannot get result type for temporal operation").

    # @spark-4: `interval year to month` renders as bare `interval` on PySpark 3.5.
    @sail-bug @spark-4
    Scenario Outline: Interval scaled by an integer: <case>
      When query
        """
        SELECT typeof(<expr>) AS t, CAST(<expr> AS STRING) AS r
        """
      Then query result
        | t   | r   |
        | <t> | <r> |

      Examples:
        | case                                        | expr                                          | t                      | r                            |
        | a year-month interval times an integer      | INTERVAL '1-2' YEAR TO MONTH * CAST(2 AS INT) | interval year to month | INTERVAL '2-4' YEAR TO MONTH |
        | a year-month interval divided by an integer | INTERVAL '2-4' YEAR TO MONTH / CAST(2 AS INT) | interval year to month | INTERVAL '1-2' YEAR TO MONTH |

  Rule: pmod operand typing after the generic numeric coercion
    # `SparkPmod` inherits DataFusion's `Signature::numeric`, which unifies both operands to
    # one common type before the remainder rule can see the originals.
    # The decimal-and-integer-column and NULL-and-string cases live in
    # `math/pmod.feature`, which also asserts the values.

    Scenario: pmod of a string and a decimal promotes to double
      When query
        """
        SELECT typeof(pmod('5.5', CAST(2.0 AS DECIMAL(10,2)))) AS t
        """
      Then query result
        | t      |
        | double |

    Scenario: pmod of Infinity and a decimal is NaN
      When query
        """
        SELECT pmod('Infinity', CAST(2.0 AS DECIMAL(10,2))) AS r
        """
      Then query result
        | r   |
        | NaN |

  Rule: The remainder type must not depend on ANSI mode
    # Spark's remainder rule is ANSI-independent, but Sail used to apply the operand
    # narrowing only under ANSI on: `decimal(10,2) % 3` came out decimal(3,2) with
    # ANSI on but decimal(10,2) with ANSI off (the untyped `nullif` zero widened the
    # divisor). The pre-existing modulo scenarios only ran in the default mode, which
    # hid this — the twins below pin both modes.

    Scenario: decimal modulo an integer literal, ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(CAST(10.5 AS DECIMAL(10,2)) % 3) AS t
        """
      Then query result
        | t            |
        | decimal(3,2) |

    Scenario: decimal modulo an integer literal, ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(10.5 AS DECIMAL(10,2)) % 3) AS t
        """
      Then query result
        | t            |
        | decimal(3,2) |

    Scenario: decimal modulo a byte column, ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(2.5 AS DECIMAL(10,2)) % CAST(3 AS TINYINT)) AS t
        """
      Then query result
        | t            |
        | decimal(5,2) |

    Scenario: modulo of a wide decimal by an integer keeps Spark's remainder precision
      # Spark: min(p1-s1, p2-s2) + max(s1,s2). Sail keeps the dividend's precision.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,18)) % CAST(3 AS TINYINT)) AS a,
               typeof(CAST(1 AS DECIMAL(20,10)) % 3) AS b,
               typeof(CAST(1 AS DECIMAL(5,0)) % 3) AS c
        """
      Then query result
        | a              | b              | c            |
        | decimal(21,18) | decimal(11,10) | decimal(1,0) |

    Scenario: modulo by NULL keeps the dividend's decimal type
      # Closed by giving `%` the same remainder re-typing `pmod` already had.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(38,18)) % NULL) AS t
        """
      Then query result
        | t              |
        | decimal(38,18) |

    Scenario: a wide decimal times NULL takes Spark's capped type
      # Companion to the decimal(38,18) case: the capping must apply to NULL operands too.
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(20,10)) * NULL) AS t
        """
      Then query result
        | t              |
        | decimal(38,17) |

  Rule: CAST of a blank or exponent string to a decimal
    # Arrow parses an empty string as ZERO for decimal targets (unlike every other numeric
    # parser, which rejects it), so a blank string would become 0.00 instead of NULL — a
    # silently wrong value, and the trim makes a whitespace-only string reach that same
    # path. A value that trims to empty is therefore intercepted before the parser.
    # Spark also accepts exponent notation for decimal targets, unlike its integer parser.

    Scenario: a blank string cast to a decimal is NULL under ANSI off
      # Both the empty and the whitespace-only string are malformed for Spark.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('' AS DECIMAL(10,2)) AS empty,
               CAST(' ' AS DECIMAL(10,2)) AS blank
        """
      Then query result
        | empty | blank |
        | NULL  | NULL  |

    Scenario: a blank string cast to a decimal raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(' ' AS DECIMAL(10,2)) AS r
        """
      Then query error CAST_INVALID_INPUT

    Scenario: a blank string column cast to a decimal is NULL under ANSI off
      # Column counterpart of the literal scenarios above: the per-row path trims with
      # `btrim`, which has the same empty-string-to-zero exposure.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(10,2)) AS r
        FROM VALUES (0, ' '), (1, ' 5 '), (2, 'x'), (3, '') AS t(i, v)
        ORDER BY i
        """
      Then query result
        | r    |
        | NULL |
        | 5.00 |
        | NULL |
        | NULL |

    Scenario: a blank string column cast to a decimal raises under ANSI on
      # The per-row path raises through a CASE over the trimmed value, so the column and
      # the literal agree; only a decimal target pays for it.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(v AS DECIMAL(10,2)) AS r FROM VALUES (0, ' ') AS t(i, v)
        """
      Then query error CAST_INVALID_INPUT

    @sail-bug
    Scenario: an exponent string cast to a decimal parses
      # Spark's decimal parser accepts exponents even though its integer parser does not.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1e2' AS DECIMAL(10,2)) AS r
        """
      Then query result
        | r      |
        | 100.00 |

  Rule: Unary minus over a string trims like the binary operators
    # `spark_unary_negate` routes the string through the same string-to-numeric helper
    # the binary operators and `CAST` use, so it applies the same trim.

    Scenario: unary minus over a padded string, ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -' 5 ' AS r
        """
      Then query result
        | r    |
        | -5.0 |

    Scenario: unary minus over a padded string, ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -' 5 ' AS r
        """
      Then query result
        | r    |
        | -5.0 |

    Scenario: unary minus over a padded string column, ANSI off
      # The column path must trim too, and a value that is only whitespace is NULL
      # rather than zero.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -v AS r FROM VALUES (0, ' 5 '), (1, ' '), (2, 'x') AS t(i, v) ORDER BY i
        """
      Then query result
        | r    |
        | -5.0 |
        | NULL |
        | NULL |

  Rule: Unary plus and positive() over a string coerce to double like unary minus
    # Validated against Spark 4.1.1: `+'5'`, `positive('5')` and `negative('5')` are
    # DOUBLE in both ANSI modes — `positive` and `negative` are `UnaryPositive` and
    # `UnaryMinus` under function names. The same string-to-numeric parse as the
    # binary operators applies: trim, NULL under ANSI off, raise under ANSI on.

    Scenario Outline: Unary plus over a string: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(<expr>) AS t, <expr> AS r
        """
      Then query result
        | t      | r   |
        | double | <r> |

      Examples:
        | case                             | ansi  | expr            | r    |
        | a padded string, ANSI off        | false | +' 5 '          | 5.0  |
        | a malformed string is NULL       | false | +'x'            | NULL |
        | a whitespace-only string is NULL | false | +' '            | NULL |
        | a padded string, ANSI on         | true  | +' 5 '          | 5.0  |
        | positive() of a padded string    | false | positive(' 5 ') | 5.0  |
        | positive() of a malformed string | false | positive('x')   | NULL |
        | positive() padded, ANSI on       | true  | positive(' 5 ') | 5.0  |
        | negative() of a padded string    | false | negative(' 5 ') | -5.0 |
        | negative() of a malformed string | false | negative('x')   | NULL |
        | negative() padded, ANSI on       | true  | negative(' 5 ') | -5.0 |

    # Both raise; the pattern is the wording both engines share (Spark:
    # CAST_INVALID_INPUT "cannot be cast", Sail: the Arrow "Cannot cast" error),
    # like the malformed binary-operand scenario above.
    Scenario Outline: A malformed string raises under ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS r
        """
      Then query error (?i)cannot (be )?cast

      Examples:
        | case       | expr          |
        | unary plus | +'x'          |
        | positive() | positive('x') |

    Scenario: unary plus over a string column, ANSI off
      # The column path goes through `btrim` rather than the plan-time trim, and a
      # value that is only whitespace is NULL rather than zero.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT +v AS r FROM VALUES (0, ' 5 '), (1, ' '), (2, 'x') AS t(i, v) ORDER BY i
        """
      Then query result
        | r    |
        | 5.0  |
        | NULL |
        | NULL |

  Rule: Division by a zero literal keeps the Spark result type
    # A plan-time zero-divisor short circuit used to replace the whole expression with an
    # untyped NULL (`void`) under ANSI off; now the non-ANSI path falls through to the
    # runtime guard, which gives the NULL the division's Spark result type, so `typeof`
    # matches Spark. Under ANSI on the literal `x / 0` still raises at plan time (where
    # Spark types the division and only fails at evaluation), so `typeof` there diverges.

    Scenario: typeof of a decimal divided by a zero literal, ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(10,2)) / 0) AS a,
               typeof(CAST(1 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2))) AS b,
               typeof(CAST(1 AS INT) / 0) AS c
        """
      Then query result
        | a             | b              | c      |
        | decimal(14,6) | decimal(23,13) | double |

    @sail-bug
    Scenario: typeof of a decimal divided by a zero literal, ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(CAST(1 AS DECIMAL(10,2)) / 0) AS a
        """
      Then query result
        | a             |
        | decimal(14,6) |

  Rule: Unary minus over a wide decimal
    # Spark's unary minus routes the value through `MathContext.DECIMAL128`, which keeps
    # only 34 significant digits — so negating a 38-digit decimal ROUNDS it, while the
    # same value read back or passed through `abs` stays exact. Sail keeps every digit,
    # which is mathematically better but diverges. Matching Spark here means deliberately
    # losing precision, so it is a maintainer call rather than an obvious fix.

    Scenario: a wide decimal is exact without negation
      When query
        """
        SELECT CAST('123456789012345678901234567890.12345678' AS DECIMAL(38,8)) AS r,
               abs(CAST('123456789012345678901234567890.12345678' AS DECIMAL(38,8))) AS a
        """
      Then query result
        | r                                       | a                                       |
        | 123456789012345678901234567890.12345678 | 123456789012345678901234567890.12345678 |

    @sail-bug
    Scenario: unary minus over a wide decimal keeps 34 significant digits
      # 30 integer digits + 4 decimals = the 34 significant digits of DECIMAL128.
      When query
        """
        SELECT -CAST('123456789012345678901234567890.12345678' AS DECIMAL(38,8)) AS r
        """
      Then query result
        | r                                        |
        | -123456789012345678901234567890.12350000 |

    @sail-bug
    Scenario: negative() over a wide decimal rounds the same way
      # 33 integer digits + 1 decimal = 34 significant digits again.
      When query
        """
        SELECT negative(CAST('123456789012345678901234567890123.45678' AS DECIMAL(38,5))) AS r
        """
      Then query result
        | r                                        |
        | -123456789012345678901234567890123.50000 |

    @sail-bug
    Scenario: unary minus over a wide decimal column rounds too
      When query
        """
        SELECT -v AS r
        FROM VALUES (CAST('123456789012345678901234567890.12345678' AS DECIMAL(38,8))) AS t(v)
        """
      Then query result
        | r                                        |
        | -123456789012345678901234567890.12350000 |
