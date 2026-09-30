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
    Scenario: TRY_CAST of an overflowing BIGINT to TIMESTAMP saturates like Spark
      When query
        """
        SELECT CAST(TRY_CAST(CAST(9223372036854775807 AS BIGINT) AS TIMESTAMP) AS BIGINT) AS result
        """
      Then query result
        | result        |
        | 9223372036854 |

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
