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
    # integral case above. Every shape of it is in the "DECIMAL to TIMESTAMP" outlines at the
    # end of this file.
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

  Rule: Decimal to double covers every kernel path

    # Sail converts through a kernel with three paths -- both operands exactly
    # representable, a single u128 division, and a string parse -- so each path is
    # probed with values only its own arithmetic can get right. Expected values come
    # from the Spark JVM over Spark Connect (UTC), compared by EQUALITY.

    Scenario Outline: decimal to double, kernel paths: <case>
      When query
        """
        SELECT CAST(CAST('<literal>' AS DECIMAL(<precision>,<scale>)) AS DOUBLE) = <expected> AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                        | literal                                  | precision | scale | expected               |
        | exact operands, one division                | 12345.67                                 | 10        | 2     | 12345.67D              |
        | exact operands at the largest scale 22      | 0.0000000000000000000001                 | 38        | 22    | 1.0E-22D               |
        | the largest exact integer 2^53 - 1          | 9007199254740991                         | 38        | 0     | 9007199254740991D      |
        | 2^53 itself                                 | 9007199254740992                         | 38        | 0     | 9007199254740992D      |
        | a tie between two doubles rounds down       | 9007199254740993                         | 38        | 0     | 9007199254740992D      |
        | a tie between two doubles rounds up         | 9007199254740995                         | 38        | 0     | 9007199254740996D      |
        | a negative wide integral part               | -123456789012345678.90                   | 38        | 2     | -1.2345678901234568E17D |
        | an exact tie at 2^60 + 128 stays even       | 1152921504606847104                      | 38        | 0     | 1.152921504606847E18D  |
        | the same tie with a remainder rounds up     | 1152921504606847104.1                    | 38        | 1     | 1.1529215046068472E18D |
        | exactly 2^60                                | 1152921504606846976                      | 38        | 0     | 1.152921504606847E18D  |
        | scale 19 with a wide integral part          | 1234567890123456789.0123456789012345678  | 38        | 19    | 1.2345678901234568E18D |
        | a tie plus 1e-19 rounds up (sticky bit)     | 1152921504606847104.0000000000000000001  | 38        | 19    | 1.1529215046068472E18D |
        | a negative tie plus 1e-19 rounds away       | -1152921504606847104.0000000000000000001 | 38        | 19    | -1.1529215046068472E18D |
        | the same tie without the remainder stays    | 1152921504606847104.0000000000000000000  | 38        | 19    | 1.152921504606847E18D  |
        | 2^53 + 1 plus 1e-19 rounds up (sticky bit)  | 9007199254740993.0000000000000000001     | 38        | 19    | 9007199254740994D      |
        | 2^53 + 1 at scale 19 without the remainder  | 9007199254740993.0000000000000000000     | 38        | 19    | 9007199254740992D      |
        | the largest DECIMAL(38,0)                   | 99999999999999999999999999999999999999   | 38        | 0     | 1.0E38D                |
        | the smallest DECIMAL(38,0)                  | -99999999999999999999999999999999999999  | 38        | 0     | -1.0E38D               |
        | all nines at scale 38                       | 0.99999999999999999999999999999999999999 | 38        | 38    | 1.0D                   |
        | zero                                        | 0                                        | 38        | 10    | 0.0D                   |
        | a tiny unscaled value at scale 20           | 0.00000000000000000001                   | 38        | 20    | 1.0E-20D               |

  Rule: Decimal to float covers every kernel path

    # Float expectations are the exact widening of Spark's float, cast back to FLOAT.

    Scenario Outline: decimal to float, kernel paths: <case>
      When query
        """
        SELECT CAST(CAST('<literal>' AS DECIMAL(<precision>,<scale>)) AS FLOAT) = CAST(<expected> AS FLOAT) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                                          | literal                                  | precision | scale | expected                    |
        | scale 22 is past the exact float powers       | 0.0000000000000000000001                 | 38        | 22    | 1.000000031374395E-22D      |
        | a double-exact integer beyond 2^24            | 9007199254740991                         | 38        | 0     | 9007199254740992.0D         |
        | a negative wide integral part                 | -123456789012345678.90                   | 38        | 2     | -1.234567905190871E17D      |
        | a double tie is not a float tie               | 1152921504606847104.1                    | 38        | 1     | 1.152921504606847E18D       |
        | scale 19 with a wide integral part            | 1234567890123456789.0123456789012345678  | 38        | 19    | 1.2345679395506094E18D      |
        | 2^24 + 1 plus 1e-19 rounds up (sticky bit)    | 16777217.0000000000000000001             | 38        | 19    | 16777218.0D                 |
        | 2^24 + 1 at scale 19 without the remainder    | 16777217.0000000000000000000             | 38        | 19    | 16777216.0D                 |
        | 2^24 + 3 at scale 19 rounds up either way     | 16777219.0000000000000000001             | 38        | 19    | 16777220.0D                 |
        | the largest DECIMAL(38,0)                     | 99999999999999999999999999999999999999   | 38        | 0     | 9.999999680285692E37D       |
        | the smallest DECIMAL(38,0)                    | -99999999999999999999999999999999999999  | 38        | 0     | -9.999999680285692E37D      |
        | scale 20 needs the string path                | 1.23456789012345678901                   | 38        | 20    | 1.2345678806304932D         |
        | the smallest DECIMAL(38,37) step              | 0.0000000000000000000000000000000000001  | 38        | 37    | 9.99999991097579E-38D       |
        | all nines at scale 38                         | 0.99999999999999999999999999999999999999 | 38        | 38    | 1.0D                        |
        | zero                                          | 0                                        | 38        | 10    | 0.0D                        |
        | a tiny unscaled value at scale 20             | 0.00000000000000000001                   | 38        | 20    | 9.999999682655225E-21D      |
        | 2^24                                          | 16777216                                 | 38        | 0     | 16777216.0D                 |
        | a tie between two floats rounds up            | 16777219                                 | 38        | 0     | 16777220.0D                 |
        | rounding the double first would pick the wrong float | 13631072.500000000514758830       | 38        | 18    | 13631073.0D                 |
        | 2^25 + 1                                      | 33554433                                 | 38        | 0     | 33554432.0D                 |
        | scale 10, the last exact float power          | 0.0000000001                             | 38        | 10    | 1.000000013351432E-10D      |
        | scale 11, the first inexact float power       | 0.00000000001                            | 38        | 11    | 9.999999960041972E-12D      |
        | a long mantissa                               | 123456789.123456789                      | 38        | 9     | 123456792.0D                |
        | one and a half                                | 1.5                                      | 38        | 1     | 1.5D                        |
        | 1e35                                          | 100000000000000000000000000000000000     | 38        | 0     | 1.0000000409184788E35D      |
        | just below the float maximum                  | 340282350000000000000000000000000000     | 38        | 0     | 3.4028234346940236E35D      |

  Rule: One batch mixes every path

    # The kernel decides per value, so a column that mixes exact, wide and extreme-scale
    # values and NULLs must come out right row by row.

    Scenario: a decimal column mixing every path casts to double
      When query
        """
        SELECT id, CAST(d AS DOUBLE) = e AS matches
        FROM VALUES
          (1, CAST('12345.67' AS DECIMAL(38,2)), 12345.67D),
          (2, CAST('123456789012345678.90' AS DECIMAL(38,2)), 1.2345678901234568E17D),
          (3, CAST(NULL AS DECIMAL(38,2)), 0.0D),
          (4, CAST('-9007199254740993' AS DECIMAL(38,2)), -9007199254740992D),
          (5, CAST('0.00' AS DECIMAL(38,2)), 0.0D),
          (6, CAST('999999999999999999999999999999999999.99' AS DECIMAL(38,2)), 1.0E36D)
          AS t(id, d, e)
        ORDER BY id
        """
      Then query result ordered
        | id | matches |
        | 1  | true    |
        | 2  | true    |
        | 3  | NULL    |
        | 4  | true    |
        | 5  | true    |
        | 6  | true    |

    Scenario: a decimal column casts to float row by row
      When query
        """
        SELECT id, CAST(d AS FLOAT) = CAST(e AS FLOAT) AS matches
        FROM VALUES
          (1, CAST('12345.67' AS DECIMAL(38,18)), 12345.669921875D),
          (2, CAST('13631072.500000000514758830' AS DECIMAL(38,18)), 13631073.0D),
          (3, CAST(NULL AS DECIMAL(38,18)), 0.0D),
          (4, CAST('16777219' AS DECIMAL(38,18)), 16777220.0D)
          AS t(id, d, e)
        ORDER BY id
        """
      Then query result ordered
        | id | matches |
        | 1  | true    |
        | 2  | true    |
        | 3  | NULL    |
        | 4  | true    |

  Rule: TRY_CAST and ANSI do not change the value

    Scenario Outline: TRY_CAST of a decimal to <target> agrees with CAST
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(CAST('123456789012345678.90' AS DECIMAL(38,2)) AS <target>) = CAST(<expected> AS <target>) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | target | ansi  | expected               |
        | DOUBLE | false | 1.2345678901234568E17D |
        | DOUBLE | true  | 1.2345678901234568E17D |
        | FLOAT  | false | 1.234567905190871E17D  |
        | FLOAT  | true  | 1.234567905190871E17D  |

    Scenario Outline: CAST of a decimal to <target> under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS <target>) = CAST(<expected> AS <target>) AS matches
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | target | ansi  | expected              |
        | DOUBLE | false | 1.0E38D               |
        | DOUBLE | true  | 1.0E38D               |
        | FLOAT  | false | 9.999999680285692E37D |
        | FLOAT  | true  | 9.999999680285692E37D |

  Rule: Decimal to float and double keep the Spark schema

    Scenario Outline: CAST and TRY_CAST nullability of a non-null decimal under ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(d AS DOUBLE) AS t, CAST(d AS DOUBLE) AS c, TRY_CAST(d AS FLOAT) AS tf, CAST(d AS FLOAT) AS cf
        FROM (SELECT CAST(1.5 AS DECIMAL(10,2)) AS d)
        """
      Then query schema
        """
        root
         |-- t: double (nullable = true)
         |-- c: double (nullable = false)
         |-- tf: float (nullable = true)
         |-- cf: float (nullable = false)
        """
      Then query result
        | t   | c   | tf  | cf  |
        | 1.5 | 1.5 | 1.5 | 1.5 |

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario: CAST of a nullable decimal column stays nullable
      When query
        """
        SELECT CAST(d AS DOUBLE) AS c, CAST(d AS FLOAT) AS cf
        FROM VALUES (CAST(1.5 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))) AS t(d)
        """
      Then query schema
        """
        root
         |-- c: double (nullable = true)
         |-- cf: float (nullable = true)
        """

    Scenario: the cast keeps the nullability of the container it sits in
      When query
        """
        SELECT array(CAST(d AS DOUBLE)) AS a, map('k', CAST(d AS FLOAT)) AS m, struct(CAST(d AS DOUBLE) AS x) AS s
        FROM (SELECT CAST(1.5 AS DECIMAL(10,2)) AS d)
        """
      Then query schema
        """
        root
         |-- a: array (nullable = false)
         |    |-- element: double (containsNull = false)
         |-- m: map (nullable = false)
         |    |-- key: string
         |    |-- value: float (valueContainsNull = false)
         |-- s: struct (nullable = false)
         |    |-- x: double (nullable = false)
        """

  # `UpCastRule` calls DATE -> TIMESTAMP an up-cast, but a DATE far outside the range a TIMESTAMP
  # holds still overflows: the TRY_CAST must return NULL, never raise.
  Rule: TRY_CAST of a DATE too large for a TIMESTAMP is NULL

    Scenario Outline: TRY_CAST of a far DATE to <type>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT TRY_CAST(date_from_unix_date(<days>) AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | type          | days       | result              |
        | TIMESTAMP     | 2000000000 | NULL                |
        | TIMESTAMP_NTZ | 2000000000 | NULL                |
        | TIMESTAMP     | 100000     | 2243-10-17 00:00:00 |

  # `Cast.decimalToTimestamp` (Cast.scala:791-793) is `(d.toBigDecimal * MICROS_PER_SECOND).longValue`.
  # `BigDecimal.longValue` drops the fraction (toward zero) and keeps only the low 64 bits of
  # what is left: it WRAPS instead of saturating or raising. There is no ANSI or TRY branch, so
  # CAST and TRY_CAST, ANSI on and off, are one rule. Values are micros since the epoch.
  #
  # Every operand is `d + CAST(id AS type)` so that the optimizer cannot fold it. A folded
  # literal runs the interpreted `eval`, whose Scala `BigDecimal.*` first rounds the product to
  # 34 significant digits; that only changes a decimal of 35 digits or more. The generated code
  # that runs on table data multiplies exactly, and that is the rule these rows pin.
  Rule: DECIMAL to TIMESTAMP truncates the fraction and wraps to 64 bits

    Scenario Outline: DECIMAL to TIMESTAMP with ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(CAST(<input> AS TIMESTAMP)) AS cast_result,
               unix_micros(TRY_CAST(<input> AS TIMESTAMP)) AS try_cast_result
        FROM range(1)
        """
      Then query result
        | cast_result | try_cast_result |
        | <result>    | <result>        |

      Examples:
        | case                     | input                                                                                          | result               |
        | no overflow              | CAST('1.5' AS DECIMAL(5,1)) + CAST(id AS DECIMAL(5,1))                                         | 1500000              |
        | negative                 | CAST('-1.5' AS DECIMAL(5,1)) + CAST(id AS DECIMAL(5,1))                                        | -1500000             |
        | fraction truncates +     | CAST('1.9999999' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                 | 1999999              |
        | fraction truncates -     | CAST('-1.9999999' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                | -1999999             |
        | sub-micro fraction       | CAST('0.0000009' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                 | 0                    |
        | high scale               | CAST('1.000000000000000001' AS DECIMAL(38,18)) + CAST(id AS DECIMAL(38,18))                    | 1000000              |
        | max long                 | CAST('9223372036854775807' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                       | -1000000             |
        | max long + 1             | CAST('9223372036854775808' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                       | 0                    |
        | min long                 | CAST('-9223372036854775808' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                      | 0                    |
        | min long - 1             | CAST('-9223372036854775809' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                      | -1000000             |
        | wraps exactly to 0       | CAST('18446744073709.551616' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                     | 0                    |
        | 38 nines                 | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 80237960547581376    |
        | -38 nines                | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))   | -80237960547581376   |
        | just below the limit     | CAST('9223372036854.775807' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                      | 9223372036854775807  |
        | just above the limit     | CAST('9223372036854.775808' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                      | -9223372036854775808 |
        | 28 digits                | CAST('1234567890123456789012345678' AS DECIMAL(28,0)) + CAST(id AS DECIMAL(28,0))              | -2413264399771308160 |
        | 29 digits                | CAST('12345678901234567890123456789' AS DECIMAL(29,0)) + CAST(id AS DECIMAL(29,0))             | -5685899923994529984 |
        | 34 digits                | CAST('1234567890123456789012345678901234' AS DECIMAL(34,0)) + CAST(id AS DECIMAL(34,0))        | -5999815502254940032 |
        | 35 digits ends 5         | CAST('12345678901234567890123456789012345' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801415745472 |
        | 35 digits ends 4         | CAST('12345678901234567890123456789012344' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801416745472 |
        | 35 digits ends 6         | CAST('12345678901234567890123456789012346' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801414745472 |
        | 36 digits                | CAST('123456789012345678901234567890123456' AS DECIMAL(36,0)) + CAST(id AS DECIMAL(36,0))      | 8761004206977200128  |
        | 37 digits                | CAST('1234567890123456789012345678901234567' AS DECIMAL(37,0)) + CAST(id AS DECIMAL(37,0))     | -4623678298768756800 |
        | 38 digits 9s then 5      | CAST('99999999999999999999999999999999999995' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 80237960543581376    |
        | 38 digits mixed          | CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 9103449233449086848  |
        | 38 digits mixed negative | CAST('-12345678901234567890123456789012345678' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))   | -9103449233449086848 |
        | tie to even up           | CAST('10000000000000000000000000000000000005' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 7386721425543678784  |
        | tie to even down         | CAST('10000000000000000000000000000000000015' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 7386721425553678784  |
        | scale 10, 38 digits      | CAST('1234567890123456789012345678.9012345678' AS DECIMAL(38,10)) + CAST(id AS DECIMAL(38,10)) | -2413264399770406926 |
        | scale 18, 38 digits      | CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)) + CAST(id AS DECIMAL(38,18)) | -7484280360333952320 |
        | null input               | CAST(NULL AS DECIMAL(10,2)) + CAST(id AS DECIMAL(10,2))                                        | NULL                 |

    Scenario Outline: DECIMAL to TIMESTAMP with ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT unix_micros(CAST(<input> AS TIMESTAMP)) AS cast_result,
               unix_micros(TRY_CAST(<input> AS TIMESTAMP)) AS try_cast_result
        FROM range(1)
        """
      Then query result
        | cast_result | try_cast_result |
        | <result>    | <result>        |

      Examples:
        | case                     | input                                                                                          | result               |
        | no overflow              | CAST('1.5' AS DECIMAL(5,1)) + CAST(id AS DECIMAL(5,1))                                         | 1500000              |
        | negative                 | CAST('-1.5' AS DECIMAL(5,1)) + CAST(id AS DECIMAL(5,1))                                        | -1500000             |
        | fraction truncates +     | CAST('1.9999999' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                 | 1999999              |
        | fraction truncates -     | CAST('-1.9999999' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                | -1999999             |
        | sub-micro fraction       | CAST('0.0000009' AS DECIMAL(10,7)) + CAST(id AS DECIMAL(10,7))                                 | 0                    |
        | high scale               | CAST('1.000000000000000001' AS DECIMAL(38,18)) + CAST(id AS DECIMAL(38,18))                    | 1000000              |
        | max long                 | CAST('9223372036854775807' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                       | -1000000             |
        | max long + 1             | CAST('9223372036854775808' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                       | 0                    |
        | min long                 | CAST('-9223372036854775808' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                      | 0                    |
        | min long - 1             | CAST('-9223372036854775809' AS DECIMAL(19,0)) + CAST(id AS DECIMAL(19,0))                      | -1000000             |
        | wraps exactly to 0       | CAST('18446744073709.551616' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                     | 0                    |
        | 38 nines                 | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 80237960547581376    |
        | -38 nines                | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))   | -80237960547581376   |
        | just below the limit     | CAST('9223372036854.775807' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                      | 9223372036854775807  |
        | just above the limit     | CAST('9223372036854.775808' AS DECIMAL(25,6)) + CAST(id AS DECIMAL(25,6))                      | -9223372036854775808 |
        | 28 digits                | CAST('1234567890123456789012345678' AS DECIMAL(28,0)) + CAST(id AS DECIMAL(28,0))              | -2413264399771308160 |
        | 29 digits                | CAST('12345678901234567890123456789' AS DECIMAL(29,0)) + CAST(id AS DECIMAL(29,0))             | -5685899923994529984 |
        | 34 digits                | CAST('1234567890123456789012345678901234' AS DECIMAL(34,0)) + CAST(id AS DECIMAL(34,0))        | -5999815502254940032 |
        | 35 digits ends 5         | CAST('12345678901234567890123456789012345' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801415745472 |
        | 35 digits ends 4         | CAST('12345678901234567890123456789012344' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801416745472 |
        | 35 digits ends 6         | CAST('12345678901234567890123456789012346' AS DECIMAL(35,0)) + CAST(id AS DECIMAL(35,0))       | -4657922801414745472 |
        | 36 digits                | CAST('123456789012345678901234567890123456' AS DECIMAL(36,0)) + CAST(id AS DECIMAL(36,0))      | 8761004206977200128  |
        | 37 digits                | CAST('1234567890123456789012345678901234567' AS DECIMAL(37,0)) + CAST(id AS DECIMAL(37,0))     | -4623678298768756800 |
        | 38 digits 9s then 5      | CAST('99999999999999999999999999999999999995' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 80237960543581376    |
        | 38 digits mixed          | CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 9103449233449086848  |
        | 38 digits mixed negative | CAST('-12345678901234567890123456789012345678' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))   | -9103449233449086848 |
        | tie to even up           | CAST('10000000000000000000000000000000000005' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 7386721425543678784  |
        | tie to even down         | CAST('10000000000000000000000000000000000015' AS DECIMAL(38,0)) + CAST(id AS DECIMAL(38,0))    | 7386721425553678784  |
        | scale 10, 38 digits      | CAST('1234567890123456789012345678.9012345678' AS DECIMAL(38,10)) + CAST(id AS DECIMAL(38,10)) | -2413264399770406926 |
        | scale 18, 38 digits      | CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)) + CAST(id AS DECIMAL(38,18)) | -7484280360333952320 |
        | null input               | CAST(NULL AS DECIMAL(10,2)) + CAST(id AS DECIMAL(10,2))                                        | NULL                 |
