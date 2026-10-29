Feature: factorial output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(5) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a non-null column input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable column input to factorial stays nullable
      When query
        """
        SELECT factorial(c) AS result FROM VALUES (5), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    # Spark declares `nullable = true` always: outside [0, 20] the result is NULL.
    Scenario Outline: output is nullable for a non-null input under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT factorial(<arg>) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

      Examples:
        | ansi  | case                | arg                    |
        | false | non-null INT column | CAST(id AS INT)        |
        | true  | non-null INT column | CAST(id AS INT)        |
        | false | non-null BIGINT col | id                     |
        | true  | non-null BIGINT col | id                     |
        | true  | non-null literal    | 5                      |
        | true  | non-null SMALLINT   | CAST(id AS SMALLINT)   |

  # Spark `Factorial` (mathExpressions.scala, v4.2.0) takes an INT (implicit cast
  # of the input) and returns BIGINT. Outside [0, 20] it returns NULL: it never
  # raises for the range, in either ANSI mode. Errors only come from the implicit
  # cast of the input to INT (CAST_OVERFLOW, CAST_INVALID_INPUT under ANSI) or from
  # a type that cannot be cast (BOOLEAN, DATE).

  Rule: Values in the supported range

    Scenario Outline: factorial of an integral input: <case>
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | arg                  | result              |
        | zero                | 0                    | 1                   |
        | five                 | 5                    | 120                 |
        | upper bound 20      | 20                   | 2432902008176640000 |
        | TINYINT             | CAST(5 AS TINYINT)   | 120                 |
        | SMALLINT            | CAST(5 AS SMALLINT)  | 120                 |
        | BIGINT in INT range | CAST(5 AS BIGINT)    | 120                 |
        | NULL                | CAST(NULL AS INT)    | NULL                |

  Rule: Out of range returns NULL and never raises

    Scenario Outline: out of range returns NULL under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  | case           | arg |
        | false | above 20       | 21  |
        | false | negative       | -1  |
        | true  | above 20       | 21  |
        | true  | negative       | -1  |

    Scenario: out of range in a column returns NULL row by row
      When query
        """
        SELECT factorial(c) AS result FROM VALUES (5), (21), (-1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query result ordered
        | result |
        | 120    |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Non-INT inputs are cast to INT like Spark

    Scenario Outline: fractional and numeric string inputs are truncated to INT under ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result |
        | 120    |

      Examples:
        | ansi  | case               | arg                      |
        | false | DECIMAL            | 5.7                      |
        | false | DOUBLE             | CAST(5.7 AS DOUBLE)      |
        | false | DECIMAL(10,0)      | CAST(5 AS DECIMAL(10,0)) |
        | false | numeric string     | '5'                      |
        | true  | DECIMAL            | 5.7                      |
        | true  | DOUBLE             | CAST(5.7 AS DOUBLE)      |
        | true  | DECIMAL(10,0)      | CAST(5 AS DECIMAL(10,0)) |
        | true  | numeric string     | '5'                      |

  Rule: The cast to INT follows ANSI mode

    Scenario Outline: ANSI off: the failed cast to INT yields NULL: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                | arg                          |
        | non-numeric string  | 'abc'                        |
        | BIGINT beyond INT   | CAST(3000000000 AS BIGINT)   |

    # Spark raises [CAST_INVALID_INPUT]; Sail raises too, but with the message of its CAST
    # ("Cannot cast string 'abc' to value of Int32 type"), without the Spark error class.
    Scenario: ANSI on: a non-numeric string raises from the cast to INT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial('abc') AS result
        """
      Then query error (?s).*(CAST_INVALID_INPUT|Cannot cast string 'abc').*

    # Spark raises [CAST_OVERFLOW]; Sail's CAST says "Can't cast value 3000000000 to type Int32".
    Scenario: ANSI on: a BIGINT beyond INT raises from the cast to INT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(CAST(3000000000 AS BIGINT)) AS result
        """
      Then query error (?s).*(CAST_OVERFLOW|Can't cast value 3000000000).*

  Rule: Types that cannot be cast to INT are rejected at analysis

    Scenario Outline: unsupported input type: <case>
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query error (?s).*DATATYPE_MISMATCH.*

      Examples:
        | case    | arg             |
        | BOOLEAN | TRUE            |
        | DATE    | DATE'2020-01-01' |

  # Spark's non-ANSI cast to INT is Java's: a string is parsed leniently (a fractional part
  # is dropped), a floating-point NaN is 0, a floating-point value saturates, and an integral
  # value keeps its low 32 bits. `factorial` then maps anything outside [0, 20] to NULL.

  Rule: The non-ANSI cast to INT keeps Java's wrapping

    Scenario Outline: ANSI off: integral values wrap to their low 32 bits: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | arg                                  | result |
        | 2^32 + 5 wraps to 5               | CAST(4294967301 AS BIGINT)           | 120    |
        | -(2^32) + 5 wraps to 5            | CAST(-4294967291 AS BIGINT)          | 120    |
        | 2^31 + 5 wraps to a negative INT  | CAST(2147483653 AS BIGINT)           | NULL   |
        | 2^32 wraps to 0                   | CAST(4294967296 AS BIGINT)           | 1      |
        | BIGINT minimum wraps to 0         | CAST(-9223372036854775808 AS BIGINT) | 1      |
        | DECIMAL 2^32 + 5 wraps to 5       | CAST(4294967301 AS DECIMAL(20,0))    | 120    |
        | DECIMAL -(2^32) + 5 wraps to 5    | CAST(-4294967291 AS DECIMAL(20,0))   | 120    |

    Scenario Outline: ANSI off: floating-point values: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | arg                       | result              |
        | DOUBLE NaN is 0         | CAST('NaN' AS DOUBLE)     | 1                   |
        | FLOAT NaN is 0          | CAST('NaN' AS FLOAT)      | 1                   |
        | DOUBLE infinity is NULL | CAST('Infinity' AS DOUBLE) | NULL               |
        | DOUBLE saturates        | CAST(1e10 AS DOUBLE)      | NULL                |
        | DOUBLE 2^32 + 5 saturates, not wraps | CAST(4294967301 AS DOUBLE) | NULL    |
        | negative fraction truncates to 0 | CAST(-0.9 AS DOUBLE) | 1                |
        | FLOAT truncates         | CAST(20.99 AS FLOAT)      | 2432902008176640000 |

    Scenario Outline: ANSI off: strings are parsed leniently: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | arg            | result              |
        | fraction is dropped          | '5.7'          | 120                 |
        | fraction dropped at the top  | '20.9'         | 2432902008176640000 |
        | surrounding whitespace       | ' 5.7 '        | 120                 |
        | trailing dot                 | '5.'           | 120                 |
        | leading dot                  | '.5'           | 1                   |
        | explicit plus sign           | '+20.9'        | 2432902008176640000 |
        | negative zero                | '-0'           | 1                   |
        | negative fraction            | '-0.5'         | 1                   |
        | exponent is malformed        | '1.5e0'        | NULL                |
        | string beyond INT            | '4294967301'   | NULL                |
        | non-ASCII digit              | '٥'            | NULL                |
        | type suffix                   | '5L'           | NULL                |
        | hexadecimal                  | '0x5'          | NULL                |
        | empty                        | ''             | NULL                |

    # Spark wraps a DECIMAL beyond BIGINT too (2^64 + 5 -> 5 -> 120). Sail casts it through
    # BIGINT, so a value that does not fit there is NULL.
    # TODO: take the decimal remainder by 2^32 first, truncate it to BIGINT and reuse the wrap. It
    # belongs with the CAST (`CAST(<decimal> AS INT)` under ANSI off), not with factorial alone.
    @sail-bug
    Scenario: ANSI off: a DECIMAL beyond BIGINT wraps to its low 32 bits
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(CAST(18446744073709551621 AS DECIMAL(38,0))) AS result
        """
      Then query result
        | result |
        | 120    |

  Rule: The ANSI cast to INT truncates fractions but rejects the rest

    Scenario Outline: ANSI on: accepted inputs: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | arg                  | result |
        | negative fraction truncates to 0  | CAST(-0.9 AS DOUBLE) | 1      |
        | surrounding whitespace            | '  5  '              | 120    |
        | negative zero                     | '-0'                 | 1      |

    # Spark raises [CAST_OVERFLOW] / [CAST_INVALID_INPUT]; Sail's CAST raises with its own text.
    Scenario Outline: ANSI on: rejected inputs raise from the cast to INT: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(<arg>) AS result
        """
      Then query error (?s).*(<code>|<message>).*

      Examples:
        | case                         | arg                        | code                | message                           |
        | BIGINT beyond INT, wrapped   | CAST(4294967301 AS BIGINT) | CAST_OVERFLOW       | Can't cast value 4294967301       |
        | DOUBLE NaN                   | CAST('NaN' AS DOUBLE)      | CAST_OVERFLOW       | Can't cast value NaN              |
        | DOUBLE beyond INT            | CAST(1e10 AS DOUBLE)       | CAST_OVERFLOW       | Can't cast value 10000000000      |
        | fractional string            | '5.7'                      | CAST_INVALID_INPUT  | Cannot cast string '5.7'          |
        | exponent string              | '1e1'                      | CAST_INVALID_INPUT  | Cannot cast string '1e1'          |
        | empty string                 | ''                         | CAST_INVALID_INPUT  | Cannot cast string ''             |

    # Spark converts a DECIMAL of more than 18 digits to DOUBLE before the range check, so a value
    # that rounds up to 2^31 raises. Sail truncates the decimal exactly (2147483647) and returns
    # NULL instead of raising.
    # TODO: raise CAST_OVERFLOW from the ANSI cast of a wide DECIMAL to INT, in the CAST itself.
    @sail-bug
    Scenario: ANSI on: a DECIMAL that rounds up to 2^31 raises CAST_OVERFLOW
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(CAST('2147483647.9999999999' AS DECIMAL(38,10))) AS result
        """
      Then query error (?s).*CAST_OVERFLOW.*
