@arithmetic_coercion
Feature: Spark type coercion for the +, -, * operators

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
    # both operands to double; ANSI on casts the string to the numeric operand's type
    # (string + decimal stays double; string + string is rejected under ANSI).

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
    # The result TYPE is @sail-bug: Sail's round() keeps the input precision
    # (decimal(23, n)) instead of Spark's decimal(11+n, n). This is a pre-existing
    # `round` divergence (the `round_decimal_base` helper exists but isn't wired) —
    # unrelated to division; kept here as the regression check for that gap.

    @sail-bug
    Scenario: round of a decimal division to a smaller scale
      When query
        """
        SELECT typeof(round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 2)) AS t,
               round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 2) AS r
        """
      Then query result
        | t             | r    |
        | decimal(13,2) | 3.33 |

    @sail-bug
    Scenario: round of a division rounds HALF_UP after dividing
      When query
        """
        SELECT typeof(round(CAST(2.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 4)) AS t,
               round(CAST(2.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 4) AS r
        """
      Then query result
        | t             | r      |
        | decimal(15,4) | 0.6667 |

    @sail-bug
    Scenario: round of a negative decimal division
      When query
        """
        SELECT typeof(round(CAST(-5.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 6)) AS t,
               round(CAST(-5.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 6) AS r
        """
      Then query result
        | t             | r         |
        | decimal(17,6) | -1.666667 |

    @sail-bug
    Scenario: round of a decimal division to zero scale
      When query
        """
        SELECT typeof(round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 0)) AS t,
               round(CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)), 0) AS r
        """
      Then query result
        | t             | r |
        | decimal(11,0) | 3 |

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

    @sail-bug
    Scenario: ANSI string plus integer widens to bigint like Spark
      Given config spark.sql.ansi.enabled = true
      # Spark casts the string operand so the result is BIGINT; Sail casts to INT.
      When query
        """
        SELECT typeof('5' + 3) AS t
        """
      Then query result
        | t      |
        | bigint |

    # `pmod` is a UDF (SparkPmod) and does not go through the arithmetic coercion,
    # so it diverges from Spark. `div` (integer division) matches Spark and needs no
    # coercion (its result is always BIGINT). Follow-up: route pmod through the same
    # operand coercion the operators use, and fix the UDF's NULL handling.

    @sail-bug
    Scenario: pmod narrows an integer literal like the remainder rule
      # Spark narrows literal 3 to Decimal(1,0) => decimal(3,2); Sail produces decimal(12,2).
      When query
        """
        SELECT typeof(pmod(CAST(10.5 AS DECIMAL(10,2)), 3)) AS t
        """
      Then query result
        | t            |
        | decimal(3,2) |

    @sail-bug
    Scenario: pmod with a NULL operand returns NULL
      # Spark returns NULL; Sail errors ("Null and Int32 are not coercible").
      When query
        """
        SELECT pmod(NULL, 3) AS r
        """
      Then query result
        | r    |
        | NULL |

    @sail-bug
    Scenario: pmod of a double and a decimal promotes to double
      # Spark promotes both to double; Sail produces decimal(30,15).
      When query
        """
        SELECT typeof(pmod(CAST(1.5 AS DOUBLE), CAST(2.0 AS DECIMAL(10,2)))) AS t
        """
      Then query result
        | t      |
        | double |
