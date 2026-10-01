Feature: CAST expressions

  Rule: Timestamp timezone conversion

    Scenario: casting TIMESTAMP_NTZ to TIMESTAMP resolves the session-zone gap and overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT label, unix_micros(CAST(value AS TIMESTAMP)) AS result
        FROM VALUES
          ('gap', TIMESTAMP_NTZ '2021-03-14 02:30:00'),
          ('overlap', TIMESTAMP_NTZ '2021-11-07 01:30:00')
          AS t(label, value)
        ORDER BY label
        """
      Then query result ordered
        | label   | result           |
        | gap     | 1615717800000000 |
        | overlap | 1636273800000000 |

    Scenario Outline: casting TIMESTAMP_NTZ to TIMESTAMP supports fixed offset <timezone>
      Given config spark.sql.session.timeZone = <timezone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          unix_micros(CAST(value AS TIMESTAMP)) AS cast_result,
          unix_micros(TRY_CAST(value AS TIMESTAMP)) AS try_result,
          CAST(CAST(NULL AS TIMESTAMP_NTZ) AS TIMESTAMP) IS NULL AS null_result
        FROM VALUES (TIMESTAMP_NTZ '1970-01-01 00:00:00') AS t(value)
        """
      Then query result
        | cast_result | try_result | null_result |
        | <result>    | <result>   | true        |

      Examples:
        | timezone | ansi  | result       |
        | +01      | true  | -3600000000  |
        | +0130    | false | -5400000000  |
        | +01:30   | true  | -5400000000  |
        | -0130    | false | 5400000000   |
        | -00:00   | true  | 0            |
        | +18:00   | false | -64800000000 |

  Rule: DATE cast ANSI behavior

    Scenario: casting a malformed string to DATE returns null when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('not-a-date' AS DATE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: casting a malformed string to DATE fails when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('not-a-date' AS DATE) AS result
        """
      Then query error CAST_INVALID_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to cast yields the schema Spark declares
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Legacy STRING to INT casts

    Scenario: decimal strings truncate and overflowing strings return NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '-4.56'),
          (3, '2147483647.999'),
          (4, '-2147483648.999'),
          (5, '2178802287'),
          (6, '2147483648'),
          (7, '-2147483649'),
          (8, '2147483648.0'),
          (9, '123.a'),
          (10, CAST(NULL AS STRING))
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result      |
        | 0  | 100         |
        | 1  | 1           |
        | 2  | -4          |
        | 3  | 2147483647  |
        | 4  | -2147483648 |
        | 5  | NULL        |
        | 6  | NULL        |
        | 7  | NULL        |
        | 8  | NULL        |
        | 9  | NULL        |
        | 10 | NULL        |

    Scenario: overflowing strings do not abort a filter predicate
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT value
        FROM VALUES ('2178802287'), ('100'), ('2147483648') AS data(value)
        WHERE CAST(value AS INT) = 100
        """
      Then query result
        | value |
        | 100   |

  Rule: ANSI and TRY casts stay strict

    Scenario Outline: ANSI CAST rejects <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<input> AS INT) AS result
        """
      Then query error <error>

      Examples:
        | case                    | input        | error      |
        | a decimal string        | '1.23'       | 1.23       |
        | an overflowing integer  | '2147483648' | 2147483648 |

    Scenario: TRY_CAST returns NULL for decimal and overflowing strings
      When query
        """
        SELECT id, TRY_CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '2147483648')
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 100    |
        | 1  | NULL   |
        | 2  | NULL   |

  Rule: Numeric to TIMESTAMP matches Spark's per-source-type overflow rule

    # Spark's `longToTimestamp` (`Cast.scala:745-752,799`) is `SECONDS.toMicros(t)`, one of
    # Java's `TimeUnit` conversions -- these saturate to `Long.MAX_VALUE`/`MIN_VALUE` on
    # overflow instead of throwing or silently wrapping, and unconditionally (no ANSI or
    # TRY_CAST branch in `castToTimestamp`'s integral arms at all).
    Scenario Outline: TRY_CAST of an overflowing BIGINT to TIMESTAMP saturates like Spark: <case>
      When query
        """
        SELECT CAST(TRY_CAST(CAST(<value> AS BIGINT) AS TIMESTAMP) AS BIGINT) AS result
        """
      Then query result
        | result         |
        | <result>       |

      Examples:
        | case                    | value                | result          |
        | positive (i64::MAX)     | 9223372036854775807  | 9223372036854   |
        | negative (i64::MIN)     | -9223372036854775808 | -9223372036855  |

    # Spark's `TimeUnit.SECONDS.toMicros` saturation is a SIGNED-integer-only Java API --
    # Spark itself has no unsigned types at all -- which is why `saturating_seconds_to_micros`'s
    # call site above guards on `is_signed_integer()`, not the broader `is_integer()` (which
    # also matches UInt8/16/32/64; an earlier round of this branch used it by mistake and then
    # fixed it). No regression test pins this distinction: for every UInt64 value large enough
    # to actually differ from a signed i64 (i.e. > i64::MAX), the seconds-to-micros product
    # already overflows i64 on ITS OWN, before either code path's saturation/non-saturation
    # logic can matter -- both the saturating and the non-saturating arm error identically on
    # such a value, so no SQL-observable case exists that discriminates the guard.

    # `decimalToTimestamp` (`Cast.scala:791-793`) is `(d.toBigDecimal * MICROS_PER_SECOND)
    # .longValue`, and `BigDecimal.longValue` WRAPS (returns only the low-order 64 bits) on
    # overflow rather than saturating -- a different, decimal-specific overflow rule from the
    # integral case above, not yet implemented. Pre-existing (also reproduces on `main`), not
    # introduced by the CAST-parity work in this branch -- left as a known gap.
    @sail-bug
    Scenario: TRY_CAST of an overflowing DECIMAL to TIMESTAMP wraps like Spark
      When query
        """
        SELECT CAST(TRY_CAST(CAST('9223372036854775807' AS DECIMAL(19,0)) AS TIMESTAMP) AS BIGINT) AS result
        """
      Then query result
        | result |
        | -1     |

    # `doubleToTimestamp` (`DateTimeUtils.scala:794-796`) is `if (d.isNaN || d.isInfinite)
    # null else (d * MICROS_PER_SECOND).toLong` -- the NaN/Infinite check is on `d` itself,
    # not on the product, so a FINITE `d` whose product overflows still reaches the
    # saturating `.toLong` narrowing (the same Java/Scala saturation the sibling
    # `castToByte/Short/Int/Long` arms already reproduce via `saturating_double_to_i64`),
    # not NULL.
    Scenario: Legacy CAST of an overflowing DOUBLE to TIMESTAMP saturates like Spark
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(CAST(CAST('1e20' AS DOUBLE) AS TIMESTAMP) AS BIGINT) AS result
        """
      Then query result
        | result        |
        | 9223372036854 |

  Rule: TIMESTAMP to DECIMAL preserves exact microsecond precision

    # `castToDecimal`'s TIMESTAMP case (`Cast.scala:1119-1121`) is `Decimal.apply(t, 19, 6)`,
    # which treats the raw microsecond `Long` as an EXACT unscaled BigDecimal -- no floating
    # point at all ("19 digits is enough to represent any TIMESTAMP value in Long", per the
    # comment there).
    Scenario: A near-year-9999 TIMESTAMP casts to DECIMAL without float rounding
      When query
        """
        SELECT CAST(CAST(TIMESTAMP '9999-12-31 23:59:59.999999' AS DECIMAL(19,6)) AS STRING) AS result
        """
      Then query result
        | result               |
        | 253402300799.999999  |

    # A pre-1970 (negative raw microsecond) TIMESTAMP: the sign-handling arithmetic this
    # exact-decimal construction needs is new and unprecedented in this file (the sibling
    # TIME->Decimal arm never needed it, since TIME is never negative), so this is its own
    # scenario rather than folded into the positive case above.
    Scenario: A pre-1970 TIMESTAMP casts to DECIMAL with the correct sign
      When query
        """
        SELECT CAST(TIMESTAMP '1969-12-31 23:59:58.999999' AS DECIMAL(19,6)) AS result
        """
      Then query result
        | result     |
        | -1.000001  |

    # `raw = -1` microsecond: truncating division gives `whole = 0` even though
    # the value is still negative, so the sign flag must come from `raw`, not `whole`.
    Scenario: A TIMESTAMP one microsecond before 1970 casts to DECIMAL without losing the sign
      When query
        """
        SELECT CAST(TIMESTAMP '1969-12-31 23:59:59.999999' AS DECIMAL(19,6)) AS result
        """
      Then query result
        | result     |
        | -0.000001  |

    # `i64::MIN` has no positive counterpart -- negating it directly (instead of
    # the much smaller whole/fraction parts) would overflow and corrupt the string.
    Scenario: The most negative representable TIMESTAMP casts to DECIMAL without corrupting the sign
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT TRY_CAST(CAST(-9223372036854775808 AS TIMESTAMP) AS DECIMAL(38,6)) AS result
        """
      Then query result
        | result                  |
        | -9223372036854.775808   |

    # Decimal-target overflow is always NUMERIC_VALUE_OUT_OF_RANGE, regardless
    # of source type. Verified against the Spark 4.2 JVM.
    Scenario: A TIMESTAMP that overflows the target DECIMAL raises NUMERIC_VALUE_OUT_OF_RANGE under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP '9999-12-31 23:59:59.999999' AS DECIMAL(5,2)) AS result
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

    # Rounding (not truncating) can push the value past the target's capacity --
    # Spark raises here, not "9.99" (the truncated-and-fit value).
    Scenario: A TIMESTAMP whose rounded (not truncated) DECIMAL value overflows raises NUMERIC_VALUE_OUT_OF_RANGE
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP '1970-01-01 00:00:09.999999' AS DECIMAL(3,2)) AS result
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

    # `precision - scale >= 19` (e.g. DECIMAL(38,19)): `10^19` overflows `i64`,
    # so a bound computed that way would itself overflow. Always valid here, since
    # a raw microsecond TIMESTAMP's whole-seconds part never reaches 19 digits.
    Scenario: A valid TIMESTAMP casts to a wide-scale DECIMAL whose overflow bound itself would overflow i64
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIMESTAMP '9999-12-31 23:59:59.999999' AS DECIMAL(38,19)) AS result
        """
      Then query result
        | result                             |
        | 253402300799.9999990000000000000   |

  Rule: Decimal to double rounds once, from the exact value

    # Decimal.toDouble converts the exact decimal in one rounding step
    # (Decimal.scala:245). Equality checks distinguish a value error from
    # a display difference, including values beyond the 53-bit exact range.

    Scenario Outline: decimal to double rounds once: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS DOUBLE) = <expected> AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                  | literal                                 | scale | expected              |
        | a wide integral part loses two digits | 123456789012345678.90                   | 2     | 1.2345678901234568E17 |
        | an exact one comes back below one     | 1                                       | 37    | 1.0D                  |
        | a long fraction loses its last digit  | 1.23456789012345678901                  | 20    | 1.2345678901234567D   |
        | the smallest DECIMAL(38,37) step      | 0.0000000000000000000000000000000000001 | 37    | 1.0E-37               |

  Rule: Doubles print the way Spark prints them

    # These scenarios assert display formatting separately from value equality.

    Scenario Outline: a double renders with an uppercase exponent: <case>
      When query
        """
        SELECT CAST(<literal> AS DOUBLE) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case                      | literal                    | expected    |
        | a large magnitude         | 1e17                       | 1.0E17      |
        | a scaled mantissa         | 1.5e20                     | 1.5E20      |
        | a small magnitude         | 1e-37                      | 1.0E-37     |
        | a float past the 1e7 mark | CAST(16777216 AS FLOAT)    | 1.6777216E7 |

  Rule: Decimal to float narrows in one step

    # Decimal.toFloat also rounds from the exact decimal (Decimal.scala:247).
    # Compare values, including the 2^24 boundary, independently of display.

    Scenario Outline: decimal to float: <case>
      When query
        """
        SELECT CAST(CAST(<literal> AS DECIMAL(38,<scale>)) AS FLOAT) = <expected> AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                              | literal               | scale | expected            |
        | a wide integral part              | 123456789012345678.90 | 2     | CAST(1.23456791E17 AS FLOAT) |
        | an exact one at maximum scale     | 1                     | 37    | CAST(1.0 AS FLOAT)  |
        | a value inside the exact range    | 12345.67              | 2     | CAST(12345.67 AS FLOAT) |
        | the 2^24 float precision boundary | 16777217              | 0     | CAST(16777216 AS FLOAT) |
