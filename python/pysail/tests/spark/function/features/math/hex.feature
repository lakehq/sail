Feature: hex function

  # Spark's `Hex` declares `inputTypes = TypeCollection(LongType, BinaryType, StringType)` with
  # `ImplicitCastInputTypes` (`mathExpressions.scala:1189-1215`), so anything else is not rejected:
  # it is CAST first, and everything Spark can turn into a string is hexed as its printed text.
  # `Hex.hex(num: Long)` sizes the output from `numberOfLeadingZeros` and never pads, so `hex(256)`
  # is three digits; zero has its own branch (`UTF8String.ZERO_UTF8`). Because the cast goes to
  # LONG, a negative TINYINT prints sixteen digits, not two -- that is what tells Spark's rule
  # apart from 'hex the bytes of the input type'.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: hex writes the shortest hexadecimal of a LONG

    Scenario Outline: the shortest form: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result |
        | <result> |

      Examples:
        | case                                     | input                     | result           |
        | zero                                     | 0                         | 0                |
        | a small value                            | 17                        | 11               |
        | the last one-byte value                  | 255                       | FF               |
        | one past a byte, an odd number of digits | 256                       | 100              |
        | the largest BIGINT                       | 9223372036854775807L      | 7FFFFFFFFFFFFFFF |
        | the smallest BIGINT                      | -9223372036854775807L - 1 | 8000000000000000 |

  # The cast is to LONG, so the two's complement is sixteen digits wide whatever the input type.

  Rule: a narrower integer is widened to LONG before it is written

    Scenario Outline: widened to LONG first: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result |
        | <result> |

      Examples:
        | case                 | input                    | result           |
        | minus one as TINYINT | CAST(-1 AS TINYINT)      | FFFFFFFFFFFFFFFF |
        | minus one as INT     | CAST(-1 AS INT)          | FFFFFFFFFFFFFFFF |
        | minus one as BIGINT  | CAST(-1 AS BIGINT)       | FFFFFFFFFFFFFFFF |
        | the smallest INT     | CAST(-2147483648 AS INT) | FFFFFFFF80000000 |

  Rule: a fractional value is truncated towards zero by the cast

    Scenario Outline: truncated by the cast: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result |
        | <result> |

      Examples:
        | case              | input                       | result           |
        | a double          | CAST(17.9 AS DOUBLE)        | 11               |
        | a negative double | CAST(-17.9 AS DOUBLE)       | FFFFFFFFFFFFFFEF |
        | a float            | CAST(17.9 AS FLOAT)         | 11               |
        | a decimal         | CAST(17.9 AS DECIMAL(10,2)) | 11               |

  Rule: a string or a binary is written byte by byte

    Scenario Outline: byte by byte: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result |
        | <result> |

      Examples:
        | case                               | input                 | result             |
        | an ASCII string                    | 'Spark SQL'           | 537061726B2053514C |
        | an empty string                    | ''                    |                    |
        | a string with a two-byte character | 'niño'                | 6E69C3B16F         |
        | a four-byte emoji                  | '😀'                  | F09F9880           |
        | a binary                           | CAST('abc' AS BINARY) | 616263             |
        | an empty binary                    | CAST('' AS BINARY)    |                    |
        | a binary that is not valid UTF-8   | X'00FF10'             | 00FF10             |

  # Nothing here is rejected for being the wrong type: Spark casts it to STRING first and hexes
  # the text it would have printed.

  Rule: any other type Spark can cast to STRING is hexed as its printed text

    Scenario Outline: hex of <case>
      Given config spark.sql.session.timeZone = UTC
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case              | input                               | result                                 |
        | BOOLEAN           | true                                | 74727565                               |
        | DATE              | DATE '2024-03-05'                   | 323032342D30332D3035                   |
        | TIMESTAMP         | TIMESTAMP '2024-03-05 06:07:08'     | 323032342D30332D30352030363A30373A3038 |
        | TIMESTAMP_NTZ     | TIMESTAMP_NTZ '2024-03-05 06:07:08' | 323032342D30332D30352030363A30373A3038 |
        | INTERVAL DAY      | INTERVAL '5' DAY                    | 494E54455256414C2027352720444159       |
        | INTERVAL YEAR     | INTERVAL '3' YEAR                   | 494E54455256414C202733272059454152     |
        | CALENDAR INTERVAL | make_interval(0,1,0,2,0,0,0)        | 31206D6F6E74687320322064617973         |

    # TIME is gated to the oracle it was measured on (Spark 4.2, `spark.sql.timeType.enabled`).
    @spark-4.2
    Scenario: hex of a TIME
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT hex(CAST('06:07:08' AS TIME(6))) AS result
        """
      Then query result collected
        | result           |
        | 30363A30373A3038 |

    @spark-4
    Scenario: hex of a VARIANT
      When query
        """
        SELECT hex(parse_json('{"a":1}')) AS result
        """
      Then query result collected
        | result         |
        | 7B2261223A317D |

  Rule: NULL travels through hex

    # An untyped NULL is a NULL STRING for Spark.
    Scenario: an untyped NULL
      When query
        """
        SELECT hex(NULL) AS result
        """
      Then query result collected
        | result |
        | NULL   |

    Scenario Outline: a typed NULL: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case      | input                |
        | as INT    | CAST(NULL AS INT)    |
        | as STRING | CAST(NULL AS STRING) |
        | as BINARY | CAST(NULL AS BINARY) |

    # The same values through a column, so the row path runs and not only constant folding.
    Scenario: hex over a column of INTs
      When query
        """
        SELECT hex(c) AS result FROM VALUES (17), (-1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result collected ordered
        | result           |
        | 11               |
        | FFFFFFFFFFFFFFFF |
        | NULL             |

  Rule: a type Spark cannot cast to STRING is rejected

    # Spark answers with an analysis error naming the function, not an internal error.
    Scenario Outline: hex of <case> is rejected
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "hex

      Examples:
        | case     | input               |
        | an array | array(1,2)          |
        | a map    | map('k',1)          |
        | a struct | named_struct('a',1) |

  # The implicit cast to LONG is an ordinary cast, so it follows ANSI: with ANSI off Spark
  # SATURATES at the BIGINT bounds (and NaN becomes zero), and only with ANSI on does it raise
  # CAST_OVERFLOW.

  Rule: the implicit cast to BIGINT overflows the way a cast does

    Scenario Outline: <case> saturates when ANSI is off
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                  | input                                         | result           |
        | a double past BIGINT  | 1.0E30D                                       | 7FFFFFFFFFFFFFFF |
        | a float past BIGINT    | CAST(1.0E30 AS FLOAT)                         | 7FFFFFFFFFFFFFFF |
        | a decimal past BIGINT | CAST('99999999999999999999' AS DECIMAL(38,0)) | 6BC75E2D630FFFFF |
        | Infinity               | CAST('Infinity' AS DOUBLE)                     | 7FFFFFFFFFFFFFFF |
        | NaN                   | CAST('NaN' AS DOUBLE)                         | 0                |

    Scenario Outline: <case> raises when ANSI is on
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case                  | input                                         |
        | a double past BIGINT  | 1.0E30D                                       |
        | a float past BIGINT    | CAST(1.0E30 AS FLOAT)                         |
        | a decimal past BIGINT | CAST('99999999999999999999' AS DECIMAL(38,0)) |
        | Infinity               | CAST('Infinity' AS DOUBLE)                     |
        | NaN                   | CAST('NaN' AS DOUBLE)                         |

    # A string is hexed byte by byte, never parsed as a number, so ANSI cannot reach it.
    Scenario Outline: a string that is not a number is unaffected by ANSI mode
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT hex('not a number') AS result
        """
      Then query result collected
        | result                   |
        | 6E6F742061206E756D626572 |

      Examples:
        | case     | ansi  |
        | ANSI on  | true  |
        | ANSI off | false |

  # Spark only evaluates the branch it takes, so a cast that would overflow in a branch that is
  # never taken is never run, with ANSI on or off.
  Rule: a branch that is never taken does not run the cast

    Scenario Outline: IF(false, ...) never evaluates the overflowing hex: <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT IF(false, hex(1.0E30D), 'x') AS result
        """
      Then query result collected
        | result |
        | x      |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: CASE never evaluates the overflowing hex: <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CASE WHEN id < 100 THEN 'x' ELSE hex(1.0E30D) END AS result FROM range(2)
        """
      Then query result collected
        | result |
        | x      |
        | x      |

      Examples:
        | ansi  |
        | true  |
        | false |

  # The same overflow through a column, so the row kernel runs and not only constant folding.
  Rule: the implicit cast to BIGINT overflows the same way over a column

    Scenario: a DOUBLE column saturates when ANSI is off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(c) AS result FROM VALUES (1.0E30D), (CAST('NaN' AS DOUBLE)), (1.0D) AS t(c)
        """
      Then query result collected ordered
        | result           |
        | 7FFFFFFFFFFFFFFF |
        | 0                |
        | 1                |

    Scenario: a DOUBLE column raises when ANSI is on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(c) AS result FROM VALUES (1.0E30D), (1.0D) AS t(c)
        """
      Then query error \[CAST_OVERFLOW\]

  # Generated from the Spark 4.2 JVM: every numeric family at each edge of BIGINT, with ANSI on and
  # off, through a literal and through a column. Spark checks `Math.floor(x) <= Long.MaxValue &&
  # Math.ceil(x) >= Long.MinValue` with both bounds promoted to double (`Cast.scala:2031`), and
  # `Long.MaxValue` rounds up to 2^63, so 2^63 itself is inside the range and saturates, and only
  # the next double above it overflows. With ANSI off nothing raises: a fractional value saturates
  # (NaN is zero) and a DECIMAL keeps the low 64 bits (`Decimal.toLong` wraps). A STRING is hexed
  # as text, never parsed as a number.
  Rule: the implicit cast to BIGINT at its edges, for every numeric type

    Scenario Outline: BIGINT edges, ANSI on, literal, value: <case>
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                                 | input                                                            | result                                 |
        | DOUBLE 2^63                          | 9.223372036854775808E18D                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 - 1024 (largest below)   | 9.223372036854774784E18D                                         | 7FFFFFFFFFFFFC00                       |
        | DOUBLE -2^63                         | -9.223372036854775808E18D                                        | 8000000000000000                       |
        | DOUBLE -2^63 + 1024 (smallest above) | -9.223372036854774784E18D                                        | 8000000000000400                       |
        | DOUBLE 0.5                           | 0.5D                                                             | 0                                      |
        | DOUBLE -0.5                          | -0.5D                                                            | 0                                      |
        | DOUBLE -0.0                          | -0.0D                                                            | 0                                      |
        | DOUBLE 1.9                           | 1.9D                                                             | 1                                      |
        | DOUBLE -1.9                          | -1.9D                                                            | FFFFFFFFFFFFFFFF                       |
        | FLOAT 2^63                           | CAST(9.223372036854775808E18D AS FLOAT)                          | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next down                 | CAST(9.223371487098962E18D AS FLOAT)                             | 7FFFFF8000000000                       |
        | FLOAT -2^63                          | CAST(-9.223372036854775808E18D AS FLOAT)                         | 8000000000000000                       |
        | FLOAT -2^63 next up                  | CAST(-9.223371487098962E18D AS FLOAT)                            | 8000008000000000                       |
        | FLOAT 16777216                       | CAST(16777216 AS FLOAT)                                          | 1000000                                |
        | FLOAT -1.9                           | CAST(-1.9D AS FLOAT)                                             | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT             | CAST('9223372036854775807' AS DECIMAL(38,0))                     | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) min BIGINT             | CAST('-9223372036854775808' AS DECIMAL(38,0))                    | 8000000000000000                       |
        | DECIMAL(38,0) zero                   | CAST('0' AS DECIMAL(38,0))                                       | 0                                      |
        | DECIMAL(38,1) max BIGINT .9          | CAST('9223372036854775807.9' AS DECIMAL(38,1))                   | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) min BIGINT .9 below    | CAST('-9223372036854775808.9' AS DECIMAL(38,1))                  | 8000000000000000                       |
        | DECIMAL(38,1) 0.9                    | CAST('0.9' AS DECIMAL(38,1))                                     | 0                                      |
        | DECIMAL(38,1) -0.9                   | CAST('-0.9' AS DECIMAL(38,1))                                    | 0                                      |
        | DECIMAL(38,18) max BIGINT frac       | CAST('9223372036854775807.999999999999999999' AS DECIMAL(38,18)) | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,18) -1 frac               | CAST('-1.999999999999999999' AS DECIMAL(38,18))                  | FFFFFFFFFFFFFFFF                       |
        | BIGINT max                           | 9223372036854775807L                                             | 7FFFFFFFFFFFFFFF                       |
        | BIGINT min                           | -9223372036854775807L - 1                                        | 8000000000000000                       |
        | STRING digits                        | '9223372036854775808'                                            | 39323233333732303336383534373735383038 |
        | STRING number-like                   | '1e30'                                                           | 31653330                               |

    Scenario Outline: BIGINT edges, ANSI on, literal, overflow: <case>
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case                               | input                                                            |
        | DOUBLE 2^63 next up                | 9.223372036854777E18D                                            |
        | DOUBLE -2^63 next down             | -9.223372036854777E18D                                           |
        | DOUBLE 1E19                        | 1.0E19D                                                          |
        | DOUBLE -1E19                       | -1.0E19D                                                         |
        | DOUBLE 1E300                       | 1.0E300D                                                         |
        | DOUBLE Infinity                    | CAST('Infinity' AS DOUBLE)                                       |
        | DOUBLE -Infinity                   | CAST('-Infinity' AS DOUBLE)                                      |
        | DOUBLE NaN                         | CAST('NaN' AS DOUBLE)                                            |
        | FLOAT 2^63 next up                 | CAST(9.223373136366403E18D AS FLOAT)                             |
        | FLOAT -2^63 next down              | CAST(-9.223373136366403E18D AS FLOAT)                            |
        | FLOAT 1E30                         | CAST(1.0E30D AS FLOAT)                                           |
        | FLOAT -1E30                        | CAST(-1.0E30D AS FLOAT)                                          |
        | FLOAT Infinity                     | CAST('Infinity' AS FLOAT)                                        |
        | FLOAT NaN                          | CAST('NaN' AS FLOAT)                                             |
        | DECIMAL(38,0) max BIGINT + 1       | CAST('9223372036854775808' AS DECIMAL(38,0))                     |
        | DECIMAL(38,0) min BIGINT - 1       | CAST('-9223372036854775809' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) 2^64                 | CAST('18446744073709551616' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) 2^64 - 1             | CAST('18446744073709551615' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) max DECIMAL(38,0)    | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))  |
        | DECIMAL(38,0) min DECIMAL(38,0)    | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) |
        | DECIMAL(38,1) max BIGINT .0 + 1    | CAST('9223372036854775808.0' AS DECIMAL(38,1))                   |
        | DECIMAL(38,1) min BIGINT - 1 .0    | CAST('-9223372036854775809.0' AS DECIMAL(38,1))                  |
        | DECIMAL(38,18) max BIGINT + 1 frac | CAST('9223372036854775808.000000000000000001' AS DECIMAL(38,18)) |

    Scenario Outline: BIGINT edges, ANSI off, literal, value: <case>
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                                 | input                                                            | result                                 |
        | DOUBLE 2^63                          | 9.223372036854775808E18D                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 next up                  | 9.223372036854777E18D                                            | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 - 1024 (largest below)   | 9.223372036854774784E18D                                         | 7FFFFFFFFFFFFC00                       |
        | DOUBLE -2^63                         | -9.223372036854775808E18D                                        | 8000000000000000                       |
        | DOUBLE -2^63 next down               | -9.223372036854777E18D                                           | 8000000000000000                       |
        | DOUBLE -2^63 + 1024 (smallest above) | -9.223372036854774784E18D                                        | 8000000000000400                       |
        | DOUBLE 1E19                          | 1.0E19D                                                          | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE -1E19                         | -1.0E19D                                                         | 8000000000000000                       |
        | DOUBLE 1E300                         | 1.0E300D                                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE Infinity                      | CAST('Infinity' AS DOUBLE)                                       | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE -Infinity                     | CAST('-Infinity' AS DOUBLE)                                      | 8000000000000000                       |
        | DOUBLE NaN                           | CAST('NaN' AS DOUBLE)                                            | 0                                      |
        | DOUBLE 0.5                           | 0.5D                                                             | 0                                      |
        | DOUBLE -0.5                          | -0.5D                                                            | 0                                      |
        | DOUBLE -0.0                          | -0.0D                                                            | 0                                      |
        | DOUBLE 1.9                           | 1.9D                                                             | 1                                      |
        | DOUBLE -1.9                          | -1.9D                                                            | FFFFFFFFFFFFFFFF                       |
        | FLOAT 2^63                           | CAST(9.223372036854775808E18D AS FLOAT)                          | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next up                   | CAST(9.223373136366403E18D AS FLOAT)                             | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next down                 | CAST(9.223371487098962E18D AS FLOAT)                             | 7FFFFF8000000000                       |
        | FLOAT -2^63                          | CAST(-9.223372036854775808E18D AS FLOAT)                         | 8000000000000000                       |
        | FLOAT -2^63 next down                | CAST(-9.223373136366403E18D AS FLOAT)                            | 8000000000000000                       |
        | FLOAT -2^63 next up                  | CAST(-9.223371487098962E18D AS FLOAT)                            | 8000008000000000                       |
        | FLOAT 1E30                           | CAST(1.0E30D AS FLOAT)                                           | 7FFFFFFFFFFFFFFF                       |
        | FLOAT -1E30                          | CAST(-1.0E30D AS FLOAT)                                          | 8000000000000000                       |
        | FLOAT Infinity                       | CAST('Infinity' AS FLOAT)                                        | 7FFFFFFFFFFFFFFF                       |
        | FLOAT NaN                            | CAST('NaN' AS FLOAT)                                             | 0                                      |
        | FLOAT 16777216                       | CAST(16777216 AS FLOAT)                                          | 1000000                                |
        | FLOAT -1.9                           | CAST(-1.9D AS FLOAT)                                             | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT             | CAST('9223372036854775807' AS DECIMAL(38,0))                     | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT + 1         | CAST('9223372036854775808' AS DECIMAL(38,0))                     | 8000000000000000                       |
        | DECIMAL(38,0) min BIGINT             | CAST('-9223372036854775808' AS DECIMAL(38,0))                    | 8000000000000000                       |
        | DECIMAL(38,0) min BIGINT - 1         | CAST('-9223372036854775809' AS DECIMAL(38,0))                    | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) 2^64                   | CAST('18446744073709551616' AS DECIMAL(38,0))                    | 0                                      |
        | DECIMAL(38,0) 2^64 - 1               | CAST('18446744073709551615' AS DECIMAL(38,0))                    | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max DECIMAL(38,0)      | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))  | 98A223FFFFFFFFF                        |
        | DECIMAL(38,0) min DECIMAL(38,0)      | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) | F675DDC000000001                       |
        | DECIMAL(38,0) zero                   | CAST('0' AS DECIMAL(38,0))                                       | 0                                      |
        | DECIMAL(38,1) max BIGINT .9          | CAST('9223372036854775807.9' AS DECIMAL(38,1))                   | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) max BIGINT .0 + 1      | CAST('9223372036854775808.0' AS DECIMAL(38,1))                   | 8000000000000000                       |
        | DECIMAL(38,1) min BIGINT .9 below    | CAST('-9223372036854775808.9' AS DECIMAL(38,1))                  | 8000000000000000                       |
        | DECIMAL(38,1) min BIGINT - 1 .0      | CAST('-9223372036854775809.0' AS DECIMAL(38,1))                  | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) 0.9                    | CAST('0.9' AS DECIMAL(38,1))                                     | 0                                      |
        | DECIMAL(38,1) -0.9                   | CAST('-0.9' AS DECIMAL(38,1))                                    | 0                                      |
        | DECIMAL(38,18) max BIGINT frac       | CAST('9223372036854775807.999999999999999999' AS DECIMAL(38,18)) | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,18) max BIGINT + 1 frac   | CAST('9223372036854775808.000000000000000001' AS DECIMAL(38,18)) | 8000000000000000                       |
        | DECIMAL(38,18) -1 frac               | CAST('-1.999999999999999999' AS DECIMAL(38,18))                  | FFFFFFFFFFFFFFFF                       |
        | BIGINT max                           | 9223372036854775807L                                             | 7FFFFFFFFFFFFFFF                       |
        | BIGINT min                           | -9223372036854775807L - 1                                        | 8000000000000000                       |
        | STRING digits                        | '9223372036854775808'                                            | 39323233333732303336383534373735383038 |
        | STRING number-like                   | '1e30'                                                           | 31653330                               |

    Scenario Outline: BIGINT edges, ANSI on, column, value: <case>
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<input>), (<input>) AS t(c)
        """
      Then query result collected
        | result   |
        | <result> |
        | <result> |

      Examples:
        | case                                 | input                                                            | result                                 |
        | DOUBLE 2^63                          | 9.223372036854775808E18D                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 - 1024 (largest below)   | 9.223372036854774784E18D                                         | 7FFFFFFFFFFFFC00                       |
        | DOUBLE -2^63                         | -9.223372036854775808E18D                                        | 8000000000000000                       |
        | DOUBLE -2^63 + 1024 (smallest above) | -9.223372036854774784E18D                                        | 8000000000000400                       |
        | DOUBLE 0.5                           | 0.5D                                                             | 0                                      |
        | DOUBLE -0.5                          | -0.5D                                                            | 0                                      |
        | DOUBLE -0.0                          | -0.0D                                                            | 0                                      |
        | DOUBLE 1.9                           | 1.9D                                                             | 1                                      |
        | DOUBLE -1.9                          | -1.9D                                                            | FFFFFFFFFFFFFFFF                       |
        | FLOAT 2^63                           | CAST(9.223372036854775808E18D AS FLOAT)                          | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next down                 | CAST(9.223371487098962E18D AS FLOAT)                             | 7FFFFF8000000000                       |
        | FLOAT -2^63                          | CAST(-9.223372036854775808E18D AS FLOAT)                         | 8000000000000000                       |
        | FLOAT -2^63 next up                  | CAST(-9.223371487098962E18D AS FLOAT)                            | 8000008000000000                       |
        | FLOAT 16777216                       | CAST(16777216 AS FLOAT)                                          | 1000000                                |
        | FLOAT -1.9                           | CAST(-1.9D AS FLOAT)                                             | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT             | CAST('9223372036854775807' AS DECIMAL(38,0))                     | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) min BIGINT             | CAST('-9223372036854775808' AS DECIMAL(38,0))                    | 8000000000000000                       |
        | DECIMAL(38,0) zero                   | CAST('0' AS DECIMAL(38,0))                                       | 0                                      |
        | DECIMAL(38,1) max BIGINT .9          | CAST('9223372036854775807.9' AS DECIMAL(38,1))                   | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) min BIGINT .9 below    | CAST('-9223372036854775808.9' AS DECIMAL(38,1))                  | 8000000000000000                       |
        | DECIMAL(38,1) 0.9                    | CAST('0.9' AS DECIMAL(38,1))                                     | 0                                      |
        | DECIMAL(38,1) -0.9                   | CAST('-0.9' AS DECIMAL(38,1))                                    | 0                                      |
        | DECIMAL(38,18) max BIGINT frac       | CAST('9223372036854775807.999999999999999999' AS DECIMAL(38,18)) | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,18) -1 frac               | CAST('-1.999999999999999999' AS DECIMAL(38,18))                  | FFFFFFFFFFFFFFFF                       |
        | BIGINT max                           | 9223372036854775807L                                             | 7FFFFFFFFFFFFFFF                       |
        | BIGINT min                           | -9223372036854775807L - 1                                        | 8000000000000000                       |
        | STRING digits                        | '9223372036854775808'                                            | 39323233333732303336383534373735383038 |
        | STRING number-like                   | '1e30'                                                           | 31653330                               |

    Scenario Outline: BIGINT edges, ANSI on, column, overflow: <case>
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<input>), (<input>) AS t(c)
        """
      Then query error \[CAST_OVERFLOW\]

      Examples:
        | case                               | input                                                            |
        | DOUBLE 2^63 next up                | 9.223372036854777E18D                                            |
        | DOUBLE -2^63 next down             | -9.223372036854777E18D                                           |
        | DOUBLE 1E19                        | 1.0E19D                                                          |
        | DOUBLE -1E19                       | -1.0E19D                                                         |
        | DOUBLE 1E300                       | 1.0E300D                                                         |
        | DOUBLE Infinity                     | CAST('Infinity' AS DOUBLE)                                        |
        | DOUBLE -Infinity                    | CAST('-Infinity' AS DOUBLE)                                       |
        | DOUBLE NaN                         | CAST('NaN' AS DOUBLE)                                            |
        | FLOAT 2^63 next up                 | CAST(9.223373136366403E18D AS FLOAT)                             |
        | FLOAT -2^63 next down              | CAST(-9.223373136366403E18D AS FLOAT)                            |
        | FLOAT 1E30                         | CAST(1.0E30D AS FLOAT)                                           |
        | FLOAT -1E30                        | CAST(-1.0E30D AS FLOAT)                                          |
        | FLOAT Infinity                      | CAST('Infinity' AS FLOAT)                                         |
        | FLOAT NaN                          | CAST('NaN' AS FLOAT)                                             |
        | DECIMAL(38,0) max BIGINT + 1       | CAST('9223372036854775808' AS DECIMAL(38,0))                     |
        | DECIMAL(38,0) min BIGINT - 1       | CAST('-9223372036854775809' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) 2^64                 | CAST('18446744073709551616' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) 2^64 - 1             | CAST('18446744073709551615' AS DECIMAL(38,0))                    |
        | DECIMAL(38,0) max DECIMAL(38,0)    | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))  |
        | DECIMAL(38,0) min DECIMAL(38,0)    | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) |
        | DECIMAL(38,1) max BIGINT .0 + 1    | CAST('9223372036854775808.0' AS DECIMAL(38,1))                   |
        | DECIMAL(38,1) min BIGINT - 1 .0    | CAST('-9223372036854775809.0' AS DECIMAL(38,1))                  |
        | DECIMAL(38,18) max BIGINT + 1 frac | CAST('9223372036854775808.000000000000000001' AS DECIMAL(38,18)) |

    Scenario Outline: BIGINT edges, ANSI off, column, value: <case>
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<input>), (<input>) AS t(c)
        """
      Then query result collected
        | result   |
        | <result> |
        | <result> |

      Examples:
        | case                                 | input                                                            | result                                 |
        | DOUBLE 2^63                          | 9.223372036854775808E18D                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 next up                  | 9.223372036854777E18D                                            | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE 2^63 - 1024 (largest below)   | 9.223372036854774784E18D                                         | 7FFFFFFFFFFFFC00                       |
        | DOUBLE -2^63                         | -9.223372036854775808E18D                                        | 8000000000000000                       |
        | DOUBLE -2^63 next down               | -9.223372036854777E18D                                           | 8000000000000000                       |
        | DOUBLE -2^63 + 1024 (smallest above) | -9.223372036854774784E18D                                        | 8000000000000400                       |
        | DOUBLE 1E19                          | 1.0E19D                                                          | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE -1E19                         | -1.0E19D                                                         | 8000000000000000                       |
        | DOUBLE 1E300                         | 1.0E300D                                                         | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE Infinity                      | CAST('Infinity' AS DOUBLE)                                       | 7FFFFFFFFFFFFFFF                       |
        | DOUBLE -Infinity                     | CAST('-Infinity' AS DOUBLE)                                      | 8000000000000000                       |
        | DOUBLE NaN                           | CAST('NaN' AS DOUBLE)                                            | 0                                      |
        | DOUBLE 0.5                           | 0.5D                                                             | 0                                      |
        | DOUBLE -0.5                          | -0.5D                                                            | 0                                      |
        | DOUBLE -0.0                          | -0.0D                                                            | 0                                      |
        | DOUBLE 1.9                           | 1.9D                                                             | 1                                      |
        | DOUBLE -1.9                          | -1.9D                                                            | FFFFFFFFFFFFFFFF                       |
        | FLOAT 2^63                           | CAST(9.223372036854775808E18D AS FLOAT)                          | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next up                   | CAST(9.223373136366403E18D AS FLOAT)                             | 7FFFFFFFFFFFFFFF                       |
        | FLOAT 2^63 next down                 | CAST(9.223371487098962E18D AS FLOAT)                             | 7FFFFF8000000000                       |
        | FLOAT -2^63                          | CAST(-9.223372036854775808E18D AS FLOAT)                         | 8000000000000000                       |
        | FLOAT -2^63 next down                | CAST(-9.223373136366403E18D AS FLOAT)                            | 8000000000000000                       |
        | FLOAT -2^63 next up                  | CAST(-9.223371487098962E18D AS FLOAT)                            | 8000008000000000                       |
        | FLOAT 1E30                           | CAST(1.0E30D AS FLOAT)                                           | 7FFFFFFFFFFFFFFF                       |
        | FLOAT -1E30                          | CAST(-1.0E30D AS FLOAT)                                          | 8000000000000000                       |
        | FLOAT Infinity                       | CAST('Infinity' AS FLOAT)                                        | 7FFFFFFFFFFFFFFF                       |
        | FLOAT NaN                            | CAST('NaN' AS FLOAT)                                             | 0                                      |
        | FLOAT 16777216                       | CAST(16777216 AS FLOAT)                                          | 1000000                                |
        | FLOAT -1.9                           | CAST(-1.9D AS FLOAT)                                             | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT             | CAST('9223372036854775807' AS DECIMAL(38,0))                     | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max BIGINT + 1         | CAST('9223372036854775808' AS DECIMAL(38,0))                     | 8000000000000000                       |
        | DECIMAL(38,0) min BIGINT             | CAST('-9223372036854775808' AS DECIMAL(38,0))                    | 8000000000000000                       |
        | DECIMAL(38,0) min BIGINT - 1         | CAST('-9223372036854775809' AS DECIMAL(38,0))                    | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) 2^64                   | CAST('18446744073709551616' AS DECIMAL(38,0))                    | 0                                      |
        | DECIMAL(38,0) 2^64 - 1               | CAST('18446744073709551615' AS DECIMAL(38,0))                    | FFFFFFFFFFFFFFFF                       |
        | DECIMAL(38,0) max DECIMAL(38,0)      | CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))  | 98A223FFFFFFFFF                        |
        | DECIMAL(38,0) min DECIMAL(38,0)      | CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)) | F675DDC000000001                       |
        | DECIMAL(38,0) zero                   | CAST('0' AS DECIMAL(38,0))                                       | 0                                      |
        | DECIMAL(38,1) max BIGINT .9          | CAST('9223372036854775807.9' AS DECIMAL(38,1))                   | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) max BIGINT .0 + 1      | CAST('9223372036854775808.0' AS DECIMAL(38,1))                   | 8000000000000000                       |
        | DECIMAL(38,1) min BIGINT .9 below    | CAST('-9223372036854775808.9' AS DECIMAL(38,1))                  | 8000000000000000                       |
        | DECIMAL(38,1) min BIGINT - 1 .0      | CAST('-9223372036854775809.0' AS DECIMAL(38,1))                  | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,1) 0.9                    | CAST('0.9' AS DECIMAL(38,1))                                     | 0                                      |
        | DECIMAL(38,1) -0.9                   | CAST('-0.9' AS DECIMAL(38,1))                                    | 0                                      |
        | DECIMAL(38,18) max BIGINT frac       | CAST('9223372036854775807.999999999999999999' AS DECIMAL(38,18)) | 7FFFFFFFFFFFFFFF                       |
        | DECIMAL(38,18) max BIGINT + 1 frac   | CAST('9223372036854775808.000000000000000001' AS DECIMAL(38,18)) | 8000000000000000                       |
        | DECIMAL(38,18) -1 frac               | CAST('-1.999999999999999999' AS DECIMAL(38,18))                  | FFFFFFFFFFFFFFFF                       |
        | BIGINT max                           | 9223372036854775807L                                             | 7FFFFFFFFFFFFFFF                       |
        | BIGINT min                           | -9223372036854775807L - 1                                        | 8000000000000000                       |
        | STRING digits                        | '9223372036854775808'                                            | 39323233333732303336383534373735383038 |
        | STRING number-like                   | '1e30'                                                           | 31653330                               |

  # An interval produced by an expression keeps its Spark fields, the way `CAST(x AS STRING)` prints
  # it: `DAY + DAY` is a DAY interval, `HOUR + MINUTE` is HOUR TO MINUTE. The result of arithmetic
  # carries no field metadata of its own, so the fields have to be recovered from the operands.
  Rule: an interval keeps its fields through expressions

    Scenario Outline: hex of an interval expression: <case>
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                                 | input                                                                                  | result                                                                     |
        | DAY + DAY                            | INTERVAL '1' DAY + INTERVAL '2' DAY                                                    | 494E54455256414C2027332720444159                                           |
        | DAY - DAY                            | INTERVAL '1' DAY - INTERVAL '2' DAY                                                    | 494E54455256414C20272D312720444159                                         |
        | negated DAY                          | -INTERVAL '5' DAY                                                                      | 494E54455256414C20272D352720444159                                         |
        | unary plus DAY                       | +INTERVAL '5' DAY                                                                      | 494E54455256414C2027352720444159                                           |
        | HOUR + MINUTE                        | INTERVAL '1' HOUR + INTERVAL '30' MINUTE                                               | 494E54455256414C202730313A33302720484F555220544F204D494E555445             |
        | DAY + HOUR                           | INTERVAL '1' DAY + INTERVAL '2' HOUR                                                   | 494E54455256414C202731203032272044415920544F20484F5552                     |
        | MINUTE + SECOND                      | INTERVAL '1' MINUTE + INTERVAL '2' SECOND                                              | 494E54455256414C202730313A303227204D494E55544520544F205345434F4E44         |
        | DAY * 3                              | INTERVAL '2' DAY * 3                                                                   | 494E54455256414C2027362030303A30303A3030272044415920544F205345434F4E44     |
        | DAY / 2                              | INTERVAL '10' DAY / 2                                                                  | 494E54455256414C2027352030303A30303A3030272044415920544F205345434F4E44     |
        | YEAR + MONTH                         | INTERVAL '1' YEAR + INTERVAL '2' MONTH                                                 | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   |
        | negated MONTH                        | -INTERVAL '5' MONTH                                                                    | 494E54455256414C20272D3527204D4F4E5448                                     |
        | coalesce of DAYs                     | coalesce(INTERVAL '1' DAY, INTERVAL '2' DAY)                                           | 494E54455256414C2027312720444159                                           |
        | CASE of DAYs                         | CASE WHEN true THEN INTERVAL '1' DAY ELSE INTERVAL '2' DAY END                         | 494E54455256414C2027312720444159                                           |
        | IF of HOURs                          | IF(true, INTERVAL '1' HOUR, INTERVAL '2' HOUR)                                         | 494E54455256414C202730312720484F5552                                       |
        | abs of a DAY                         | abs(INTERVAL '-5' DAY)                                                                 | 494E54455256414C2027352720444159                                           |
        | timestamp difference                 | TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'                      | 494E54455256414C2027312030303A30303A3030272044415920544F205345434F4E44     |
        | timestamp difference with a fraction | TIMESTAMP '2024-01-02 00:00:00.5' - TIMESTAMP '2024-01-01 00:00:00'                    | 494E54455256414C2027312030303A30303A30302E35272044415920544F205345434F4E44 |
        | make_dt_interval                     | make_dt_interval(1, 2, 3, 4)                                                           | 494E54455256414C2027312030323A30333A3034272044415920544F205345434F4E44     |
        | make_ym_interval                     | make_ym_interval(1, 2)                                                                 | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   |
        | difference plus DAY                  | (TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00') + INTERVAL '1' DAY | 494E54455256414C2027322030303A30303A3030272044415920544F205345434F4E44     |

    Scenario: an interval added in a column keeps its fields
      When query
        """
        SELECT hex(a + b) AS result FROM VALUES (INTERVAL '1' DAY, INTERVAL '2' DAY) AS t(a, b)
        """
      Then query result collected
        | result   |
        | 494E54455256414C2027332720444159 |

    Scenario: a negated interval column keeps its fields
      When query
        """
        SELECT hex(-a) AS result FROM VALUES (INTERVAL '1' DAY, INTERVAL '2' DAY) AS t(a, b)
        """
      Then query result collected
        | result   |
        | 494E54455256414C20272D312720444159 |

    Scenario Outline: hex of an interval aggregate: <function>
      When query
        """
        SELECT hex(<function>(c)) AS result FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(c)
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | function | result                           |
        | sum      | 494E54455256414C2027332720444159 |
        | max      | 494E54455256414C2027322720444159 |

    # The variable of a lambda carries no field metadata, so the fields come from the other operand.
    Scenario: an interval computed inside a lambda keeps its fields
      When query
        """
        SELECT transform(array(INTERVAL '1' DAY), x -> hex(x + INTERVAL '2' DAY))[0] AS result
        """
      Then query result collected
        | result                           |
        | 494E54455256414C2027332720444159 |

  # `Hex` only casts a calendar interval to STRING when ANSI is on; with ANSI off it is outside the
  # `TypeCollection` like any other type. The year-month and day-time intervals are not affected.
  Rule: a calendar interval needs ANSI to be cast to STRING

    Scenario: ANSI on hexes the printed text
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query result collected
        | result                         |
        | 31206D6F6E74687320322064617973 |

    Scenario: ANSI off rejects it
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "hex

    Scenario: ANSI off still hexes a day-time interval
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(INTERVAL '5' DAY) AS result
        """
      Then query result collected
        | result                           |
        | 494E54455256414C2027352720444159 |

  Rule: hex takes exactly one argument

    Scenario Outline: hex with <case>
      When query template
        """
        SELECT <call> AS result
        """
      Then query error \[WRONG_NUM_ARGS\.WITHOUT_SUGGESTION\] The `hex` requires 1 parameters

      Examples:
        | case          | call      |
        | no arguments  | hex()     |
        | two arguments | hex(1, 2) |

  # Each of these already matched Spark by accident (the old signature rejected the type, or the
  # schema was loose); they are pinned here so a change to the coercion cannot move them unseen.
  Rule: results that must not move

    Scenario: a TIMESTAMP is printed in the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT hex(TIMESTAMP '2024-03-10 02:30:00') AS result
        """
      Then query result collected
        | result                                 |
        | 323032342D30332D31302030333A33303A3030 |

    Scenario: an interval column keeps its fields and its NULL
      When query
        """
        SELECT hex(c) AS result FROM VALUES (INTERVAL '5' DAY), (NULL) AS t(c)
        """
      Then query result collected ordered
        | result                           |
        | 494E54455256414C2027352720444159 |
        | NULL                             |

    Scenario: a year-month interval column keeps its fields
      When query
        """
        SELECT hex(c) AS result FROM VALUES (INTERVAL '3' YEAR), (INTERVAL '1-2' YEAR TO MONTH) AS t(c)
        """
      Then query result collected ordered
        | result                                                   |
        | 494E54455256414C2027332D3027205945415220544F204D4F4E5448 |
        | 494E54455256414C2027312D3227205945415220544F204D4F4E5448 |

    Scenario: a decimal is truncated towards zero, so minus 0.99 is zero
      When query
        """
        SELECT hex(CAST(-0.99 AS DECIMAL(10,2))) AS result
        """
      Then query result collected
        | result |
        | 0      |

    Scenario: the column of an outer join keeps the unmatched row as NULL
      When query
        """
        SELECT hex(b.v) AS result
        FROM VALUES (1), (2) AS a(k)
        LEFT JOIN VALUES (1, CAST('ab' AS BINARY)) AS b(k, v) ON a.k = b.k
        ORDER BY a.k
        """
      Then query result collected ordered
        | result |
        | 6162   |
        | NULL   |

    Scenario: a decimal that wraps keeps the low 64 bits, without leading zeros
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(c) AS result FROM VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))) AS t(c)
        """
      Then query result collected
        | result          |
        | 98A223FFFFFFFFF |

  # `to_char(binary, 'hex')` and `sha2` print through the same implementation as `hex`.
  Rule: the callers that share the implementation

    @spark-4
    Scenario: to_char hex over a BINARY column
      When query
        """
        SELECT to_char(c, 'hex') AS result FROM VALUES (X'00FF'), (X''), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query result collected ordered
        | result |
        | 00FF   |
        |        |
        | NULL   |

    Scenario: sha2 over a STRING column
      When query
        """
        SELECT sha2(s, 256) AS result FROM VALUES ('abc'), (''), ('niño'), (NULL) AS t(s)
        """
      Then query result collected ordered
        | result                                                           |
        | ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad |
        | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |
        | d6108ffef03cf18f9cee2f691c4eb52ac74432f0d12e04497312032c10d9f273 |
        | NULL                                                             |

  # Spark casts these to STRING and hexes the text, so a column keeps its NULLs.
  Rule: a column of a type Spark casts to STRING is hexed as text

    Scenario: a BOOLEAN column
      When query
        """
        SELECT hex(c) AS result FROM VALUES (true), (false) AS t(c)
        """
      Then query result collected ordered
        | result     |
        | 74727565   |
        | 66616C7365 |

    Scenario: a DATE column with a NULL
      When query
        """
        SELECT hex(c) AS result FROM VALUES (DATE'2024-01-02'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query result collected ordered
        | result               |
        | 323032342D30312D3032 |
        | NULL                 |

  # A column with NULLs in every position: first, middle, last and alternating, and a column that is
  # all NULL. Generated from the Spark 4.2 JVM for every input family. The NULL rows must stay NULL
  # and must not disturb their neighbours, and with ANSI on a NULL row must not raise.
  Rule: a column with NULLs, for every input type

    Scenario Outline: NULL first: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (NULL), (<a>), (<b>) AS t(c)
        """
      Then query result collected ordered
        | result |
        | <r1> |
        | <r2> |
        | <r3> |

      Examples:
        | case                   | a                                     | b                                      | r1   | r2                                                                         | r3                                                                       | ansi  |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | NULL | 5                                                                          | FFFFFFFFFFFFFFFF                                                         | false |
        | INT                    | 17                                    | -1                                     | NULL | 11                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | BIGINT                 | 255L                                  | -1L                                    | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | false |
        | STRING                 | 'ab'                                  | 'niño'                                 | NULL | 6162                                                                       | 6E69C3B16F                                                               | false |
        | BINARY                 | X'00FF'                               | X''                                    | NULL | 00FF                                                                       |                                                                          | false |
        | BOOLEAN                | true                                  | false                                  | NULL | 74727565                                                                   | 66616C7365                                                               | false |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | NULL | 323032342D30312D3032                                                       | 303030312D30312D3031                                                     | false |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | NULL | 323032342D30312D30322030333A30343A30352E35                                 | 313936392D31322D33312032333A35393A3539                                   | false |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | NULL | 323032342D30312D30322030333A30343A3035                                     | 313936392D31322D33312032333A35393A35392E3235                             | false |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | NULL | 494E54455256414C2027352720444159                                           | 494E54455256414C20272D322720444159                                       | false |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | NULL | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | false |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | NULL | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | false |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | NULL | 5                                                                          | FFFFFFFFFFFFFFFF                                                         | true  |
        | INT                    | 17                                    | -1                                     | NULL | 11                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | BIGINT                 | 255L                                  | -1L                                    | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | NULL | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | true  |
        | STRING                 | 'ab'                                  | 'niño'                                 | NULL | 6162                                                                       | 6E69C3B16F                                                               | true  |
        | BINARY                 | X'00FF'                               | X''                                    | NULL | 00FF                                                                       |                                                                          | true  |
        | BOOLEAN                | true                                  | false                                  | NULL | 74727565                                                                   | 66616C7365                                                               | true  |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | NULL | 323032342D30312D3032                                                       | 303030312D30312D3031                                                     | true  |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | NULL | 323032342D30312D30322030333A30343A30352E35                                 | 313936392D31322D33312032333A35393A3539                                   | true  |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | NULL | 323032342D30312D30322030333A30343A3035                                     | 313936392D31322D33312032333A35393A35392E3235                             | true  |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | NULL | 494E54455256414C2027352720444159                                           | 494E54455256414C20272D322720444159                                       | true  |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | NULL | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | true  |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | NULL | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | true  |

    Scenario Outline: NULL in the middle: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<a>), (NULL), (<b>) AS t(c)
        """
      Then query result collected ordered
        | result |
        | <r1> |
        | <r2> |
        | <r3> |

      Examples:
        | case                   | a                                     | b                                      | r1                                                                         | r2   | r3                                                                       | ansi  |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | INT                    | 17                                    | -1                                     | 11                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | false |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | NULL | 6E69C3B16F                                                               | false |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       | NULL |                                                                          | false |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | NULL | 66616C7365                                                               | false |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | NULL | 303030312D30312D3031                                                     | false |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | NULL | 313936392D31322D33312032333A35393A3539                                   | false |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | NULL | 313936392D31322D33312032333A35393A35392E3235                             | false |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | NULL | 494E54455256414C20272D322720444159                                       | false |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | NULL | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | false |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | NULL | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | false |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | INT                    | 17                                    | -1                                     | 11                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | true  |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | NULL | 6E69C3B16F                                                               | true  |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       | NULL |                                                                          | true  |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | NULL | 66616C7365                                                               | true  |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | NULL | 303030312D30312D3031                                                     | true  |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | NULL | 313936392D31322D33312032333A35393A3539                                   | true  |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | NULL | 313936392D31322D33312032333A35393A35392E3235                             | true  |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | NULL | 494E54455256414C20272D322720444159                                       | true  |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | NULL | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | true  |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | NULL | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | true  |

    Scenario Outline: NULL last: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<a>), (<b>), (NULL) AS t(c)
        """
      Then query result collected ordered
        | result |
        | <r1> |
        | <r2> |
        | <r3> |

      Examples:
        | case                   | a                                     | b                                      | r1                                                                         | r2                                                                       | r3   | ansi  |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | INT                    | 17                                    | -1                                     | 11                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | false |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | 6E69C3B16F                                                               | NULL | false |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       |                                                                          | NULL | false |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | 66616C7365                                                               | NULL | false |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | 303030312D30312D3031                                                     | NULL | false |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | 313936392D31322D33312032333A35393A3539                                   | NULL | false |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | 313936392D31322D33312032333A35393A35392E3235                             | NULL | false |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | 494E54455256414C20272D322720444159                                       | NULL | false |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | NULL | false |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | NULL | false |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | INT                    | 17                                    | -1                                     | 11                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | FFFFFFFFFFFFFFFF                                                         | NULL | true  |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | 6E69C3B16F                                                               | NULL | true  |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       |                                                                          | NULL | true  |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | 66616C7365                                                               | NULL | true  |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | 303030312D30312D3031                                                     | NULL | true  |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | 313936392D31322D33312032333A35393A3539                                   | NULL | true  |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | 313936392D31322D33312032333A35393A35392E3235                             | NULL | true  |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | 494E54455256414C20272D322720444159                                       | NULL | true  |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | NULL | true  |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | NULL | true  |

    Scenario Outline: alternating NULLs: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<a>), (NULL), (<b>), (NULL), (<a>) AS t(c)
        """
      Then query result collected ordered
        | result |
        | <r1> |
        | <r2> |
        | <r3> |
        | <r4> |
        | <r5> |

      Examples:
        | case                   | a                                     | b                                      | r1                                                                         | r2   | r3                                                                       | r4   | r5                                                                         | ansi  |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | 5                                                                          | false |
        | INT                    | 17                                    | -1                                     | 11                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | 11                                                                         | false |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | false |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | false |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | false |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | false |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | false |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | NULL | 6E69C3B16F                                                               | NULL | 6162                                                                       | false |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       | NULL |                                                                          | NULL | 00FF                                                                       | false |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | NULL | 66616C7365                                                               | NULL | 74727565                                                                   | false |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | NULL | 303030312D30312D3031                                                     | NULL | 323032342D30312D3032                                                       | false |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | NULL | 313936392D31322D33312032333A35393A3539                                   | NULL | 323032342D30312D30322030333A30343A30352E35                                 | false |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | NULL | 313936392D31322D33312032333A35393A35392E3235                             | NULL | 323032342D30312D30322030333A30343A3035                                     | false |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | NULL | 494E54455256414C20272D322720444159                                       | NULL | 494E54455256414C2027352720444159                                           | false |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | NULL | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | NULL | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | false |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | NULL | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | NULL | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | false |
        | TINYINT                | CAST(5 AS TINYINT)                    | CAST(-1 AS TINYINT)                    | 5                                                                          | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | 5                                                                          | true  |
        | INT                    | 17                                    | -1                                     | 11                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | 11                                                                         | true  |
        | BIGINT                 | 255L                                  | -1L                                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | true  |
        | FLOAT                  | CAST(255.9 AS FLOAT)                  | CAST(-1.5 AS FLOAT)                    | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | true  |
        | DOUBLE                 | 255.9D                                | -1.5D                                  | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | true  |
        | DECIMAL(10,2)          | CAST(255.99 AS DECIMAL(10,2))         | CAST(-1.50 AS DECIMAL(10,2))           | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | true  |
        | DECIMAL(38,0)          | CAST('255' AS DECIMAL(38,0))          | CAST('-1' AS DECIMAL(38,0))            | FF                                                                         | NULL | FFFFFFFFFFFFFFFF                                                         | NULL | FF                                                                         | true  |
        | STRING                 | 'ab'                                  | 'niño'                                 | 6162                                                                       | NULL | 6E69C3B16F                                                               | NULL | 6162                                                                       | true  |
        | BINARY                 | X'00FF'                               | X''                                    | 00FF                                                                       | NULL |                                                                          | NULL | 00FF                                                                       | true  |
        | BOOLEAN                | true                                  | false                                  | 74727565                                                                   | NULL | 66616C7365                                                               | NULL | 74727565                                                                   | true  |
        | DATE                   | DATE '2024-01-02'                     | DATE '0001-01-01'                      | 323032342D30312D3032                                                       | NULL | 303030312D30312D3031                                                     | NULL | 323032342D30312D3032                                                       | true  |
        | TIMESTAMP              | TIMESTAMP '2024-01-02 03:04:05.5'     | TIMESTAMP '1969-12-31 23:59:59'        | 323032342D30312D30322030333A30343A30352E35                                 | NULL | 313936392D31322D33312032333A35393A3539                                   | NULL | 323032342D30312D30322030333A30343A30352E35                                 | true  |
        | TIMESTAMP_NTZ          | TIMESTAMP_NTZ '2024-01-02 03:04:05'   | TIMESTAMP_NTZ '1969-12-31 23:59:59.25' | 323032342D30312D30322030333A30343A3035                                     | NULL | 313936392D31322D33312032333A35393A35392E3235                             | NULL | 323032342D30312D30322030333A30343A3035                                     | true  |
        | INTERVAL DAY           | INTERVAL '5' DAY                      | INTERVAL '-2' DAY                      | 494E54455256414C2027352720444159                                           | NULL | 494E54455256414C20272D322720444159                                       | NULL | 494E54455256414C2027352720444159                                           | true  |
        | INTERVAL YEAR TO MONTH | INTERVAL '1-2' YEAR TO MONTH          | INTERVAL '-3-0' YEAR TO MONTH          | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | NULL | 494E54455256414C20272D332D3027205945415220544F204D4F4E5448               | NULL | 494E54455256414C2027312D3227205945415220544F204D4F4E5448                   | true  |
        | INTERVAL DAY TO SECOND | INTERVAL '1 02:03:04.5' DAY TO SECOND | INTERVAL '-0 00:00:01' DAY TO SECOND   | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | NULL | 494E54455256414C20272D302030303A30303A3031272044415920544F205345434F4E44 | NULL | 494E54455256414C2027312030323A30333A30342E35272044415920544F205345434F4E44 | true  |

    Scenario Outline: a column that is all NULL: <case>
      When query template
        """
        SELECT hex(c) AS result FROM VALUES (<null>), (<null>), (<null>) AS t(c)
        """
      Then query result collected
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

      Examples:
        | case                   | null                                 |
        | TINYINT                | CAST(NULL AS TINYINT)                |
        | INT                    | CAST(NULL AS INT)                    |
        | BIGINT                 | CAST(NULL AS BIGINT)                 |
        | FLOAT                  | CAST(NULL AS FLOAT)                  |
        | DOUBLE                 | CAST(NULL AS DOUBLE)                 |
        | DECIMAL(10,2)          | CAST(NULL AS DECIMAL(10,2))          |
        | DECIMAL(38,0)          | CAST(NULL AS DECIMAL(38,0))          |
        | STRING                 | CAST(NULL AS STRING)                 |
        | BINARY                 | CAST(NULL AS BINARY)                 |
        | BOOLEAN                | CAST(NULL AS BOOLEAN)                |
        | DATE                   | CAST(NULL AS DATE)                   |
        | TIMESTAMP              | CAST(NULL AS TIMESTAMP)              |
        | TIMESTAMP_NTZ          | CAST(NULL AS TIMESTAMP_NTZ)          |
        | INTERVAL DAY           | CAST(NULL AS INTERVAL DAY)           |
        | INTERVAL YEAR TO MONTH | CAST(NULL AS INTERVAL YEAR TO MONTH) |
        | INTERVAL DAY TO SECOND | CAST(NULL AS INTERVAL DAY TO SECOND) |

    # `nullif` leaves NULL rows behind: a NULL row must not raise even when the value it replaced would
    # have overflowed the BIGINT cast.
    Scenario Outline: a NULL made from an overflowing value does not raise, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT hex(nullif(c, 1.0E30D)) AS result FROM VALUES (1.0E30D), (2.5D), (1.0E30D) AS t(c)
        """
      Then query result collected ordered
        | result |
        | NULL   |
        | 2      |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: a DECIMAL NULL made from an overflowing value does not raise, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT hex(nullif(c, CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) AS result
        FROM VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('7' AS DECIMAL(38,0))) AS t(c)
        """
      Then query result collected ordered
        | result |
        | NULL   |
        | 7      |

      Examples:
        | ansi  |
        | true  |
        | false |

    # TIME is gated to the oracle it was measured on (Spark 4.2, `spark.sql.timeType.enabled`).
    @spark-4.2
    Scenario: a TIME column with a NULL in the middle
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT hex(c) AS result FROM VALUES (CAST('06:07:08' AS TIME(6))), (NULL), (CAST('23:59:59.5' AS TIME(6))) AS t(c)
        """
      Then query result collected ordered
        | result                   |
        | 30363A30373A3038         |
        | NULL                     |
        | 32333A35393A35392E35     |

  # unhex is the inverse, so the pair must round-trip. An odd number of digits is padded on the
  # LEFT (`Hex.unhex` builds the head from a single nibble), and an invalid digit gives NULL.
  Rule: hex and unhex round-trip

    Scenario: a string survives hex and unhex
      When query
        """
        SELECT decode(unhex(hex('Spark SQL')), 'UTF-8') AS result
        """
      Then query result collected
        | result    |
        | Spark SQL |

    Scenario Outline: round trip: <case>
      When query template
        """
        SELECT hex(unhex(<input>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                                          | input | result |
        | an odd number of digits is padded on the left | 'ABC' | 0ABC   |
        | an invalid digit gives NULL                   | 'ZZ'  | NULL   |

  # `QueryExecutionErrors.castingCauseOverflowError`: the value is shown as the SOURCE type prints it
  # (`1.0E30` for a FLOAT, `1.0E30D` for a DOUBLE, `…BD` for a DECIMAL with its fraction) and the
  # message ends with the `try_cast` hint.
  Rule: the overflow error shows the source value and type

    Scenario Outline: ANSI on: hex of <case> overflowing a BIGINT
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value <shown> of the type "<type>" cannot be cast to "BIGINT" due to an overflow\. Use `try_cast` to tolerate overflow and return NULL instead\.

      Examples:
        | case      | input                                         | shown                  | type            |
        | a DOUBLE  | 1.0E30D                                       | 1.0E30D                | DOUBLE          |
        | a DECIMAL | CAST('99999999999999999999' AS DECIMAL(38,0)) | 99999999999999999999BD | DECIMAL.38,0.   |

    Scenario: ANSI on: hex of a FLOAT overflowing a BIGINT names FLOAT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(CAST(1.0E30 AS FLOAT)) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value 1\.0E30 of the type "FLOAT" cannot be cast to "BIGINT" due to an overflow\. Use `try_cast` to tolerate overflow and return NULL instead\.

    Scenario: ANSI on: hex of a fractional DECIMAL overflowing a BIGINT keeps its fraction
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(CAST('9223372036854775808.9' AS DECIMAL(38,1))) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value 9223372036854775808\.9BD of the type "DECIMAL\(38,1\)" cannot be cast to "BIGINT" due to an overflow

  # `VariantGet.cast` to STRING: a string variant is its raw text (no quotes) and a variant null is
  # SQL NULL; an object or array is its JSON.
  Rule: a VARIANT is hexed as its text

    @spark-4
    Scenario Outline: hex of a VARIANT <case>
      When query template
        """
        SELECT hex(parse_json('<json>')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case   | json | result |
        | string | "ab" | 6162   |
        | null   | null | NULL   |

  # The Spark fields of an interval (`DAY`) are carried by the column, so they survive a projection,
  # a CTE and a join. Sail does not keep them in the field of the result of interval arithmetic, so
  # `hex` of such a column prints the widest day-time type (`CAST` has the same gap).
  Rule: an interval keeps its fields across a projection

    @sail-bug
    Scenario Outline: hex of an interval computed in <case>
      When query template
        """
        <query>
        """
      Then query result collected
        | result                           |
        | 494E54455256414C2027332720444159 |

      Examples:
        | case       | query                                                                                                                                  |
        | a subquery | SELECT hex(x) AS result FROM (SELECT INTERVAL '1' DAY + INTERVAL '2' DAY AS x)                                                         |
        | a CTE      | WITH t AS (SELECT INTERVAL '1' DAY + INTERVAL '2' DAY AS x) SELECT hex(x) AS result FROM t                                             |
        | a join     | SELECT hex(l.x) AS result FROM (SELECT 1 AS k, INTERVAL '1' DAY + INTERVAL '2' DAY AS x) l JOIN (SELECT 1 AS k) r ON l.k = r.k          |

  # `QueryErrorsBase.toSQLValue`: NaN and the infinities print bare, a finite DOUBLE gets `D`, a
  # FLOAT has no suffix and a DECIMAL ends in `BD`; the sign is kept.
  Rule: the overflow error prints every kind of source value

    Scenario Outline: ANSI on: hex of <case> overflowing a BIGINT, non-finite and negative
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value <shown> of the type "<type>" cannot be cast to "BIGINT" due to an overflow

      Examples:
        | case               | input                                          | shown                   | type            |
        | NaN                | CAST('NaN' AS DOUBLE)                          | NaN                     | DOUBLE          |
        | a negative DOUBLE  | -1.0E30D                                       | -1.0E30D                | DOUBLE          |
        | a negative FLOAT   | CAST('-Infinity' AS FLOAT)                     | -Infinity               | FLOAT           |
        | a negative DECIMAL | CAST('-99999999999999999999' AS DECIMAL(38,0)) | -99999999999999999999BD | DECIMAL.38,0.   |

  Rule: a VARIANT is hexed as the text its cast prints

    @spark-4
    Scenario Outline: hex of a VARIANT <case> prints as its cast text
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case      | input                                       | result                                 |
        | true      | parse_json('true')                          | 74727565                               |
        | a double  | parse_json('1.5e10')                        | 312E35453130                           |
        | a date    | CAST(DATE'2024-01-02' AS VARIANT)           | 323032342D30312D3032                   |
        | timestamp | CAST(TIMESTAMP'2024-01-02 03:04:05' AS VARIANT) | 323032342D30312D30322030333A30343A3035 |

  @function(nullability)
  Rule: hex of a VARIANT is nullable

    @spark-4
    Scenario: a variant null
      When query
        """
        SELECT hex(parse_json('null')) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    @spark-4
    Scenario: a non-null column
      When query
        """
        SELECT hex(parse_json(CAST(id AS STRING))) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: a computed interval is printed the same by CAST, hex and to_binary

    @sail-bug
    Scenario: a day-time interval computed in a subquery
      When query
        """
        SELECT CAST(x AS STRING) AS c, hex(x) AS h, hex(to_binary(x, 'utf-8')) AS b
        FROM (SELECT INTERVAL '1' DAY + INTERVAL '2' DAY AS x)
        """
      Then query result collected
        | c                | h                                | b                                |
        | INTERVAL '3' DAY | 494E54455256414C2027332720444159 | 494E54455256414C2027332720444159 |

    Scenario: a year-month interval computed in a subquery
      When query
        """
        SELECT hex(x) AS result FROM (SELECT INTERVAL '1' YEAR + INTERVAL '2' MONTH AS x)
        """
      Then query result collected
        | result                                                   |
        | 494E54455256414C2027312D3227205945415220544F204D4F4E5448 |

    Scenario: a difference of timestamps keeps DAY TO SECOND
      When query
        """
        SELECT CAST(x AS STRING) AS result
        FROM (SELECT TIMESTAMP '2024-01-02 00:00:00' - TIMESTAMP '2024-01-01 00:00:00' AS x)
        """
      Then query result collected
        | result                               |
        | INTERVAL '1 00:00:00' DAY TO SECOND |

  @function(nullability)
  Rule: hex of a FLOAT is nullable

    Scenario: a non-null FLOAT column
      When query
        """
        SELECT hex(CAST(id AS FLOAT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  # `avg` returns the full DAY TO SECOND (or YEAR TO MONTH) type (`Average.scala:70-78`), whatever the
  # fields of its input. Pinned because the interval fields travel in field metadata.
  @function(nullability)
  Rule: the type of an aggregate of intervals follows Spark's rule for that aggregate

    Scenario Outline: <function> of an HOUR interval
      When query template
        """
        SELECT typeof(<function>(c)) AS result FROM VALUES (INTERVAL '1' HOUR) AS t(c)
        """
      Then query result collected
        | result |
        | <type> |

      Examples:
        | function | type                   |
        | avg      | interval day to second |

    Scenario: avg of an HOUR interval has the full day-time schema
      When query
        """
        SELECT avg(c) AS result FROM VALUES (INTERVAL '1' HOUR), (INTERVAL '3' HOUR) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

  Rule: an interval keeps its fields in the shapes a relation can give it

    Scenario: a day-time interval plus a difference of timestamps
      When query
        """
        SELECT hex(INTERVAL '1' DAY + (TIMESTAMP '2024-01-02' - TIMESTAMP '2024-01-01')) AS result
        """
      Then query result collected
        | result                                                               |
        | 494E54455256414C2027322030303A30303A3030272044415920544F205345434F4E44 |

    @sail-bug
    Scenario: two relations that both call the interval column x
      When query
        """
        SELECT hex(l.x) AS a, hex(r.x) AS b
        FROM (SELECT 1 k, INTERVAL '1' DAY + INTERVAL '1' DAY x) l
        JOIN (SELECT 1 k, INTERVAL '1' HOUR + INTERVAL '1' HOUR x) r ON l.k = r.k
        """
      Then query result collected
        | a                                | b                                    |
        | 494E54455256414C2027322720444159 | 494E54455256414C202730322720484F5552 |

  Rule: an overflow on a later row of a column raises or saturates there

    Scenario: ANSI on: the third row overflows
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(c) AS result FROM VALUES (1.0D), (2.0D), (1.0E30D) AS t(c)
        """
      Then query error \[CAST_OVERFLOW\] The value 1.0E30D of the type "DOUBLE"

    Scenario: ANSI off: the third row saturates and the others are untouched
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(c) AS result FROM VALUES (1, 1.0D), (2, 2.0D), (3, 1.0E30D) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result           |
        | 1                |
        | 2                |
        | 7FFFFFFFFFFFFFFF |

  # `CastCheckResult`/`ExpectsInputTypes` print the expression and the SQL type of the input, not the
  # Arrow type: `however "array(1, 2)" has the type "ARRAY<INT>"`.
  Rule: the rejection of a type prints Spark's expression and type

    @sail-bug
    Scenario: hex of an ARRAY
      When query
        """
        SELECT hex(array(1, 2)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "hex\(array\(1, 2\)\)".* however "array\(1, 2\)" has the type "ARRAY<INT>"

  # `VariantGet.cast` to STRING: a BINARY variant keeps its bytes and a timestamp nested in an
  # object or array prints in the session time zone.
  Rule: a VARIANT of another kind is hexed as Spark prints it

    @spark-4
    @sail-bug
    Scenario: a BINARY variant keeps its bytes
      When query
        """
        SELECT hex(CAST(X'414243' AS VARIANT)) AS result
        """
      Then query result collected
        | result |
        | 414243 |

    @spark-4
    @sail-bug
    Scenario: a timestamp inside an array prints in the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT hex(CAST(array(TIMESTAMP '2024-01-02 03:04:05') AS VARIANT)) AS result
        """
      Then query result collected
        | result                                                           |
        | 5B22323032342D30312D30322030333A30343A30352D30383A3030225D |

  # A filter, a limit or an offset hands the function a slice of the batch: the rows it sees are not
  # the first rows of the buffers. `LIMIT .. OFFSET ..` below the projection builds that slice.
  Rule: a column that has been sliced or reduced to NULL

    Scenario Outline: hex of a STRING column sliced with an offset, <case>
      When query template
        """
        SELECT hex(c) AS result
        FROM (SELECT c FROM VALUES (<a>), (<b>), (<c>), (<d>) AS t(c) LIMIT 3 OFFSET 1)
        """
      Then query result ordered
        | result   |
        | <first>  |
        | <second> |
        | <third>  |

      Examples:
        | case   | a                     | b                       | c                       | d                     | first | second | third |
        | STRING | 'a'                   | 'bb'                    | CAST(NULL AS STRING)    | 'c'                   | 6262  | NULL   | 63    |
        | BINARY | X'61'                 | X'6262'                 | CAST(NULL AS BINARY)    | X'63'                 | 6262  | NULL   | 63    |
        | BIGINT | 1                     | 255                     | CAST(NULL AS BIGINT)    | 16                    | FF    | NULL   | 10    |

    Scenario: hex of a column that is all NULL
      When query
        """
        SELECT hex(c) AS result
        FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: hex of CHAR keeps the text

    Scenario: a CHAR is hexed as its text
      When query
        """
        SELECT hex(CAST('ab' AS CHAR(5))) AS result
        """
      Then query result collected
        | result |
        | 6162   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(17) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null column input to hex yields the schema Spark declares
      When query
        """
        SELECT hex(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to hex stays nullable
      When query
        """
        SELECT hex(c) AS result FROM VALUES (17), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # Float/Double -> Integral is force-nullable (Cast.scala:446)

    Scenario Outline: hex without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT hex(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 17    |

    Scenario Outline: hex through a force-nullable implicit cast: <case>
      When query
        """
        SELECT hex(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | case             | input              |
        | DOUBLE -> BIGINT | CAST(17 AS DOUBLE) |
