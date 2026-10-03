Feature: bin converts integral values to binary strings

  Rule: String arguments follow Spark cast semantics

    Scenario: bin trims strings before casting to integers
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES (' 13 '), (' -13 ') AS data(value)
        ORDER BY value
        """
      Then query result
        | result                                                           |
        | 1111111111111111111111111111111111111111111111111111111111110011 |
        | 1101                                                             |

    Scenario Outline: String ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case                                                | input                  |
        | bin malformed string errors under ANSI on           | 'ab'                   |
        | bin empty string errors under ANSI on               | ''                     |
        | bin decimal string errors under ANSI on             | '13.9'                 |
        | bin out-of-range string errors under ANSI on        | '99999999999999999999' |
        | bin scientific notation string errors under ANSI on | '1e3'                  |

    Scenario Outline: String ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                | input                  |
        | bin empty string returns NULL under ANSI off        | ''                     |
        | bin out-of-range string returns NULL under ANSI off | '99999999999999999999' |

    Scenario: bin malformed string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |

    Scenario: bin decimal string truncates toward zero under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result
        FROM VALUES (0, '13.9'), (1, '-13.9'), (2, '.3') AS data(id, value)
        ORDER BY id
        """
      Then query result
        | result                                                           |
        | 1101                                                             |
        | 1111111111111111111111111111111111111111111111111111111111110011 |
        | 0                                                                |

    Scenario: bin scientific notation strings return NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(value) AS result FROM VALUES
          (0, '1e3'),
          (1, '1E3'),
          (2, '  1e3  ')
        AS t(id, value) ORDER BY id
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Integer boundaries

    Scenario Outline: Integer boundary: <case>
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | input                                | result                                                           |
        | bin zero                                       | 0                                    | 0                                                                |
        | bin minus one is all ones                      | -1                                   | 1111111111111111111111111111111111111111111111111111111111111111 |
        | bin INT_MAX uses 31 bits                       | 2147483647                           | 1111111111111111111111111111111                                  |
        | bin INT_MIN sign-extends to 64 bits            | -2147483648                          | 1111111111111111111111111111111110000000000000000000000000000000 |
        | bin LONG_MAX uses 63 bits                      | 9223372036854775807L                 | 111111111111111111111111111111111111111111111111111111111111111  |
        | bin LONG_MIN is 1 followed by 63 zeros         | CAST(-9223372036854775808 AS BIGINT) | 1000000000000000000000000000000000000000000000000000000000000000 |
        | bin TINYINT input promotes to BIGINT semantics | CAST(99 AS TINYINT)                  | 1100011                                                          |

  Rule: Floating-point truncation toward zero

    Scenario Outline: Truncation: <case>
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                    | input                     | result                                                           |
        | bin DOUBLE 1.5 truncates to 1                           | 1.5                       | 1                                                                |
        | bin DOUBLE 0.5 truncates to 0                           | 0.5                       | 0                                                                |
        | bin DOUBLE -0.5 truncates toward zero                   | -0.5                      | 0                                                                |
        | bin DOUBLE -1.5 truncates toward zero then sign-extends | -1.5                      | 1111111111111111111111111111111111111111111111111111111111111111 |
        | bin DECIMAL truncates fractional part                   | CAST(1.5 AS DECIMAL(3,2)) | 1                                                                |

  Rule: NULL inputs

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                               | input                |
        | bin untyped NULL returns NULL      | NULL                 |
        | bin typed NULL BIGINT returns NULL | CAST(NULL AS BIGINT) |

  Rule: Unsupported types are rejected

    Scenario Outline: Unsupported type: <case>
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query error .*

      Examples:
        | case                  | input                           |
        | bin rejects BOOLEAN   | true                            |
        | bin rejects DATE      | DATE '2024-01-15'               |
        | bin rejects TIMESTAMP | TIMESTAMP '2024-01-15 12:00:00' |
        | bin rejects BINARY    | X'01'                           |
        | bin rejects ARRAY     | array(1)                        |
        | bin rejects MAP       | map('a', 1)                     |
        | bin rejects STRUCT    | named_struct('a', 1)            |

  Rule: Float NaN and Infinity follow cast semantics

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query error CAST_OVERFLOW

      Examples:
        | case                                         | input                      |
        | bin NaN errors under ANSI on                 | CAST('NaN' AS DOUBLE)      |
        | bin Infinity errors under ANSI on            | CAST('Infinity' AS DOUBLE) |
        | bin out-of-range DOUBLE errors under ANSI on | CAST(1e30 AS DOUBLE)       |

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                         | input                      | result                                                          |
        | bin NaN truncates to zero under ANSI off                     | CAST('NaN' AS DOUBLE)      | 0                                                               |
        | bin Infinity saturates to LONG_MAX under ANSI off            | CAST('Infinity' AS DOUBLE) | 111111111111111111111111111111111111111111111111111111111111111 |
        | bin out-of-range DOUBLE saturates to LONG_MAX under ANSI off | CAST(1e30 AS DOUBLE)       | 111111111111111111111111111111111111111111111111111111111111111 |

  # `Cast.scala:2031` compares `Math.floor(x) <= Long.MaxValue` with the bound promoted to double,
  # and `Long.MaxValue` rounds up to 2^63: 2^63 itself is accepted (it saturates) and the next
  # double above it, 2^63 + 2048, overflows. The lower bound is exactly -2^63.
  Rule: the BIGINT bounds of a DOUBLE are 2^63 and -2^63

    Scenario Outline: ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query template
        """
        SELECT bin(CAST(<input> AS DOUBLE)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ansi  | case                    | input                | result                                                           |
        | true  | 2^63 saturates          | 9223372036854775808  | 111111111111111111111111111111111111111111111111111111111111111  |
        | true  | -2^63 is exact          | -9223372036854775808 | 1000000000000000000000000000000000000000000000000000000000000000 |
        | false | 2^63 saturates          | 9223372036854775808  | 111111111111111111111111111111111111111111111111111111111111111  |
        | false | above 2^63 saturates    | 9223372036854777856  | 111111111111111111111111111111111111111111111111111111111111111  |
        | false | -2^63 is exact          | -9223372036854775808 | 1000000000000000000000000000000000000000000000000000000000000000 |
        | false | below -2^63 saturates   | -9223372036854777856 | 1000000000000000000000000000000000000000000000000000000000000000 |

    Scenario Outline: ANSI on: bin at the BIGINT bounds: <case>
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT bin(CAST(<input> AS DOUBLE)) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value <shown>D of the type "DOUBLE" cannot be cast to "BIGINT" due to an overflow\. Use `try_cast` to tolerate overflow and return NULL instead\.

      Examples:
        | case                | input                | shown                 |
        | above 2^63 errors   | 9223372036854777856  | 9.223372036854778E18  |
        | below -2^63 errors  | -9223372036854777856 | -9.223372036854778E18 |

  # `bin` casts a DECIMAL to BIGINT as `hex` does: the fraction is truncated, then ANSI off keeps the
  # low 64 bits (`Decimal.toLong`) and ANSI on raises `CAST_OVERFLOW`.
  Rule: a DECIMAL wider than a BIGINT follows the cast

    Scenario: ANSI off: a DECIMAL beyond BIGINT keeps its low 64 bits
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(CAST('99999999999999999999' AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result                                                          |
        | 110101111000111010111100010110101100011000011111111111111111111 |

    Scenario: ANSI on: a DECIMAL beyond BIGINT overflows
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(CAST('99999999999999999999' AS DECIMAL(38,0))) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value 99999999999999999999BD of the type "DECIMAL\(38,0\)"

  Rule: bin casts every numeric type as Spark does

    Scenario: ANSI off: a DECIMAL column wraps and keeps small values
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT bin(c) AS result
        FROM VALUES (1, CAST('99999999999999999999' AS DECIMAL(38,0))), (2, CAST('7' AS DECIMAL(38,0))) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                                          |
        | 110101111000111010111100010110101100011000011111111111111111111 |
        | 111                                                             |

    Scenario: ANSI on: a DECIMAL column overflows
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bin(c) AS result FROM VALUES (CAST('99999999999999999999' AS DECIMAL(38,0))) AS t(c)
        """
      Then query error \[CAST_OVERFLOW\] The value 99999999999999999999BD of the type "DECIMAL.38,0."

    Scenario Outline: ANSI on: bin of <case> overflowing a BIGINT
      Given config spark.sql.ansi.enabled = true
      When query template
        """
        SELECT bin(<input>) AS result
        """
      Then query error \[CAST_OVERFLOW\] The value <shown> of the type "<type>" cannot be cast to "BIGINT" due to an overflow

      Examples:
        | case     | input                       | shown    | type   |
        | a FLOAT  | CAST(1.0E30 AS FLOAT)       | 1.0E30   | FLOAT  |
        | Infinity | CAST('Infinity' AS DOUBLE)  | Infinity | DOUBLE |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null integer literal yields a non-nullable string
      When query
        """
        SELECT bin(5) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null integer column yields a non-nullable string
      When query
        """
        SELECT bin(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable integer column stays nullable
      When query
        """
        SELECT bin(c) AS result FROM VALUES (5), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
