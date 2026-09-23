Feature: CAST(x AS STRING) renders each type the way Spark does
  # Spark routes both `CAST(x AS STRING)` and `show()` through one trait, ToStringBase
  # (sql/catalyst/.../expressions/ToStringBase.scala), but the two implementors override
  # four members differently. Three of those overrides are visible below and are the
  # reason a single shared renderer cannot be correct for both paths:
  #
  #   member                  Cast                       ToPrettyString (= show())
  #   nullString              "null"                     "NULL"
  #   binaryFormatter         raw bytes                  [53 70 61 72 6B] hex-discrete
  #   useDecimalPlainString   ansiEnabled                always true
  #
  # For FLOAT and DOUBLE there is no Spark-side formatting code at all: the contract is
  # literally java.lang.Double.toString / Float.toString, whose shape is fixed by javadoc —
  # scientific notation outside [1e-3, 1e7), an uppercase `E`, no `+` on the exponent, a
  # mandatory fraction digit, and the words NaN / Infinity / -Infinity.
  #
  # All expected values measured on Spark JVM 4.2.0, session time zone UTC.

  Rule: DOUBLE and FLOAT follow java.lang.Double.toString exactly
    # The thresholds are the whole point: 1e-3 stays decimal and 1e-4 flips to scientific;
    # 9999999.0 stays decimal and 1e7 flips. A renderer that never uses scientific notation
    # passes neither boundary, and one that always uses it fails the decimal half — so the
    # rows are paired on purpose. 0.1+0.2 is deliberately NOT here: both engines agree on
    # it, so the case everyone reaches for proves nothing. Every input below is explicitly
    # cast to DOUBLE: a bare literal like `9999999.0` is DECIMAL(8,1) in Spark SQL, which
    # renders plainly and would silently test the wrong renderer.

    Scenario Outline: DOUBLE rendering: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | input                            | result    |
        | whole value keeps a .0 suffix   | CAST(8 AS DOUBLE)                | 8.0       |
        | positive zero                   | CAST(0.0 AS DOUBLE)              | 0.0       |
        | a round hundred is not special  | CAST(100 AS DOUBLE)              | 100.0     |
        | just below the upper threshold  | CAST(9999999.0 AS DOUBLE)        | 9999999.0 |
        | at the lower threshold          | 1e-3                             | 0.001     |

    @sail-bug
    Scenario Outline: DOUBLE rendering Sail gets wrong: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | input                       | result                  |
        | at the upper threshold, scientific  | 1e7                         | 1.0E7                   |
        | one step below the lower threshold  | 1e-4                        | 1.0E-4                  |
        | small negative exponent             | 1.0E-5                      | 1.0E-5                  |
        | large positive exponent             | 1e100                       | 1.0E100                 |
        | large negative value                | -1e100                      | -1.0E100                |
        | uppercase E on MAX_VALUE            | 1.7976931348623157E308      | 1.7976931348623157E308  |
        | uppercase E on MIN_NORMAL           | 2.2250738585072014E-308     | 2.2250738585072014E-308 |
        | smallest subnormal                  | 4.9E-324                    | 4.9E-324                |
        | positive infinity is a word         | CAST('Infinity' AS DOUBLE)  | Infinity                |
        | negative infinity is a word         | CAST('-Infinity' AS DOUBLE) | -Infinity               |
        | nine-digit whole value goes sci     | CAST(123456789.0 AS DOUBLE) | 1.23456789E8            |

    Scenario: NaN renders as NaN
      # The one special value both engines already spell the same way; kept as the
      # contrasting half of the Infinity pair above.
      When query
        """
        SELECT CAST(CAST('NaN' AS DOUBLE) AS STRING) AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario Outline: FLOAT uses the SAME thresholds as DOUBLE: <case>
      # Float.toString switches to scientific at the identical 1e-3 / 1e7 boundaries, so a
      # renderer that special-cases only f64 diverges here.
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | input                      | result       |
        | at the upper threshold   | CAST(1e7 AS FLOAT)         | 1.0E7        |
        | float MAX_VALUE          | CAST(3.4028235E38 AS FLOAT) | 3.4028235E38 |
        | float smallest subnormal | CAST(1.4E-45 AS FLOAT)     | 1.4E-45      |
        | float infinity           | CAST('Infinity' AS FLOAT)  | Infinity     |

  Rule: A NULL nested in a container renders lowercase under CAST and uppercase under show
    # This is the sharpest CAST-vs-show split. Cast overrides nullString to "null" and
    # ToPrettyString to "NULL", so the SAME value has two spellings depending on the path.
    # Sail emits the show() spelling from the CAST path.
    #
    # Note the position asymmetry Spark keeps in both modes: the element at index 0 gets no
    # leading space, later ones get one from the separator — hence "[null, null]".

    @sail-bug
    Scenario Outline: NULL inside a container cast to string: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | input                                      | result       |
        | NULL in the middle       | array(1, CAST(NULL AS INT), 3)             | [1, null, 3] |
        | all-NULL array           | array(CAST(NULL AS INT), CAST(NULL AS INT)) | [null, null] |
        | NULL struct field        | named_struct('a',1,'b',CAST(NULL AS INT))  | {1, null}    |
        | NULL map value           | map('k', CAST(NULL AS STRING))             | {k -> null}  |

    Scenario: a top-level NULL cast to string is a real NULL, not the text
      # The contrasting half: CAST(NULL AS STRING) short-circuits in UnaryExpression.eval
      # and never reaches the renderer, so it is a SQL NULL. Only a NULL *nested inside* a
      # container becomes the four-character token.
      When query
        """
        SELECT CAST(NULL AS STRING) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: BINARY casts to raw bytes, not to hex
    # Cast does not override binaryFormatter, so it inherits UTF8String.fromBytes — the
    # bytes are reinterpreted as UTF-8 with no validation and no copy. Only show() applies
    # the [53 70 61 72 6B] hex-discrete formatter. Sail uses the hex form in the CAST path.

    Scenario: a top-level binary casts to its bytes
      When query
        """
        SELECT CAST(X'537061726B' AS STRING) AS result
        """
      Then query result
        | result |
        | Spark  |

    @sail-bug
    Scenario: binary nested in an array casts to its bytes too
      # The nested case is what discriminates: an implementation can get the top level
      # right by special-casing it and still hand the container branch a hex formatter.
      When query
        """
        SELECT CAST(array(X'6162', X'63') AS STRING) AS result
        """
      Then query result
        | result  |
        | [ab, c] |

  Rule: Container syntax
    # Field names never appear for a struct, nested strings are never quoted or escaped,
    # and map keys keep insertion order rather than being sorted.

    Scenario Outline: Container rendering: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | input                       | result           |
        | struct drops the field names     | named_struct('x',1,'y','a') | {1, a}           |
        | nested strings are not quoted    | array('a, b','c')           | [a, b, c]        |
        | map keeps insertion order        | map(2,'b',1,'a')            | {2 -> b, 1 -> a} |
        | nested arrays                    | array(array(1,2), array(3)) | [[1, 2], [3]]    |
        | a map inside a struct            | named_struct('a', map(1,'a')) | {{1 -> a}}     |

  Rule: DECIMAL keeps its declared scale
    # toPlainString prints the declared scale, trailing zeros included — so the scale is
    # visible in the output and a renderer that normalises the value loses it.

    Scenario Outline: DECIMAL rendering: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | input                      | result |
        | trailing zero is kept       | CAST(1.10 AS DECIMAL(3,2)) | 1.10   |
        | integer gains its scale     | CAST(1 AS DECIMAL(10,2))   | 1.00   |
        | decimal has no signed zero  | CAST(-0.0 AS DECIMAL(2,1)) | 0.0    |

  Rule: Interval strings pad leading hours minutes and seconds

    Scenario Outline: Interval rendering: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS literal_value, CAST(v AS STRING) AS column_value
        FROM VALUES (<input>) AS t(v)
        """
      Then query result
        | literal_value | column_value |
        | <result>      | <result>     |

      Examples:
        | case                    | input                                   | result                                  |
        | day stays unpadded      | INTERVAL '1' DAY                        | INTERVAL '1' DAY                        |
        | single hour             | INTERVAL '1' HOUR                       | INTERVAL '01' HOUR                      |
        | hour to minute          | INTERVAL '1:02' HOUR TO MINUTE           | INTERVAL '01:02' HOUR TO MINUTE          |
        | negative hour to second | INTERVAL '-1:02:03.4' HOUR TO SECOND     | INTERVAL '-01:02:03.4' HOUR TO SECOND    |
        | single minute           | INTERVAL '1' MINUTE                     | INTERVAL '01' MINUTE                    |
        | minute to second        | INTERVAL '1:02.3' MINUTE TO SECOND       | INTERVAL '01:02.3' MINUTE TO SECOND      |
        | single second           | INTERVAL '1' SECOND                     | INTERVAL '01' SECOND                    |
        | fractional second       | INTERVAL '-0.000001' SECOND             | INTERVAL '-00.000001' SECOND             |
        | zero second             | INTERVAL '0' SECOND                     | INTERVAL '00' SECOND                    |
        | large leading hour      | INTERVAL '123:04' HOUR TO MINUTE         | INTERVAL '123:04' HOUR TO MINUTE         |

  Rule: Temporal types print a fraction only when it is non-zero
    # appendFraction(NANO_OF_SECOND, 0, 9, true) has minWidth 0, so a zero sub-second part
    # prints neither the dot nor any digit; a non-zero one prints 1..9 digits with trailing
    # zeros stripped. The pair below is what discriminates a fixed-width formatter (which
    # would print .000000 and .100000) from Spark's variable-width one.

    Scenario Outline: Temporal rendering: <case>
      When query
        """
        SELECT CAST(<input> AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | input                                      | result                     |
        | zero fraction prints no dot     | TIMESTAMP '2020-01-01 00:00:00'            | 2020-01-01 00:00:00        |
        | trailing zeros are stripped     | TIMESTAMP '2020-01-01 00:00:00.100'        | 2020-01-01 00:00:00.1      |
        | six digits when they are needed | TIMESTAMP_NTZ '1970-01-01 00:00:00.000001' | 1970-01-01 00:00:00.000001 |
        | full microsecond precision      | TIMESTAMP '2020-01-01 00:00:00.123456'     | 2020-01-01 00:00:00.123456 |
        | years below 1000 are zero-padded | DATE '0015-01-01'                         | 0015-01-01                 |
        | first representable date        | DATE '0001-01-01'                          | 0001-01-01                 |
