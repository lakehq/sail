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
        | a float           | CAST(17.9 AS FLOAT)         | 11               |
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
        | a four-byte emoji                  | '😀'                   | F09F9880           |
        | a binary                           | CAST('abc' AS BINARY) | 616263             |
        | an empty binary                    | CAST('' AS BINARY)    |                    |
        | a binary that is not valid UTF-8   | X'00FF10'             | 00FF10             |

  # Nothing here is rejected for being the wrong type: Spark casts it to STRING first and hexes
  # the text it would have printed. Sail declares the three types literally and matches no
  # signature for any of these.

  Rule: any other type Spark can cast to STRING is hexed as its printed text

    @sail-bug
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

    # TIME only exists from Spark 4.2 on.
    @spark-4.2
    @sail-bug
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
    @sail-bug
    Scenario: hex of a VARIANT
      When query
        """
        SELECT hex(parse_json('{"a":1}')) AS result
        """
      Then query result collected
        | result         |
        | 7B2261223A317D |

  Rule: NULL travels through hex

    # An untyped NULL is a NULL STRING for Spark; Sail refuses the Null type outright.
    @sail-bug
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

    # Spark answers with an analysis error naming the function; Sail leaks DataFusion's
    # `Internal error: Function 'hex' failed to match any signature`.
    @sail-bug
    Scenario Outline: hex of <case> is rejected
      When query template
        """
        SELECT hex(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\]

      Examples:
        | case     | input               |
        | an array | array(1,2)          |
        | a map    | map('k',1)          |
        | a struct | named_struct('a',1) |

  # The implicit cast to LONG is an ordinary cast, so it follows ANSI: with ANSI off Spark
  # SATURATES at the BIGINT bounds (and NaN becomes zero), and only with ANSI on does it raise
  # CAST_OVERFLOW. Sail raises either way, so the ANSI-off half is a wrong answer, not a
  # different message.

  Rule: the implicit cast to BIGINT overflows the way a cast does

    @sail-bug
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
        | a float past BIGINT   | CAST(1.0E30 AS FLOAT)                         | 7FFFFFFFFFFFFFFF |
        | a decimal past BIGINT | CAST('99999999999999999999' AS DECIMAL(38,0)) | 6BC75E2D630FFFFF |
        | Infinity              | CAST('Infinity' AS DOUBLE)                    | 7FFFFFFFFFFFFFFF |
        | NaN                   | CAST('NaN' AS DOUBLE)                         | 0                |

    @sail-bug
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
        | a float past BIGINT   | CAST(1.0E30 AS FLOAT)                         |
        | a decimal past BIGINT | CAST('99999999999999999999' AS DECIMAL(38,0)) |
        | Infinity              | CAST('Infinity' AS DOUBLE)                    |
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

  @function(nullability)
  Rule: Output schema

    @sail-bug
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

    @sail-bug
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
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
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
