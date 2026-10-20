Feature: unary minus (negative) honors ANSI overflow semantics

  Rule: Negating the minimum integral value overflows

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -CAST(<value> AS <type>) AS result
        """
      Then query error (?i)overflow

      Examples:
        | case                                     | value                | type     |
        | negate INT_MIN errors under ANSI on      | -2147483648          | INT      |
        | negate BIGINT_MIN errors under ANSI on   | -9223372036854775808 | BIGINT   |
        | negate SMALLINT_MIN errors under ANSI on | -32768               | SMALLINT |
        | negate TINYINT_MIN errors under ANSI on  | -128                 | TINYINT  |

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -CAST(<value> AS <type>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | value                | type     | result               |
        | negate INT_MIN wraps under ANSI off      | -2147483648          | INT      | -2147483648          |
        | negate BIGINT_MIN wraps under ANSI off   | -9223372036854775808 | BIGINT   | -9223372036854775808 |
        | negate SMALLINT_MIN wraps under ANSI off | -32768               | SMALLINT | -32768               |
        | negate TINYINT_MIN wraps under ANSI off  | -128                 | TINYINT  | -128                 |

  Rule: Ordinary negation is unaffected by ANSI mode

    Scenario Outline: Ordinary negation: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                       | expr                         | result |
        | negate a positive integer                  | -CAST(5 AS INT)              | -5     |
        | double negation returns the original value | -(-CAST(5 AS INT))           | 5      |
        | negate a double                            | -CAST(1.5 AS DOUBLE)         | -1.5   |
        | negate a decimal                           | -CAST(1.50 AS DECIMAL(10,2)) | -1.50  |
        | negate NULL returns NULL                   | -CAST(NULL AS INT)           | NULL   |

  Rule: Floating-point negation never overflows

    Scenario Outline: Floating-point: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | expr                        | result    |
        | negate a float                            | -CAST(1.5 AS FLOAT)         | -1.5      |
        | negate positive zero yields negative zero | -CAST(0.0 AS DOUBLE)        | -0.0      |
        | negate NaN returns NaN                    | -CAST('NaN' AS DOUBLE)      | NaN       |
        | negate Infinity returns negative Infinity | -CAST('Infinity' AS DOUBLE) | -Infinity |

    Scenario: negate a float column negates each row
      When query
        """
        SELECT id, -v AS result FROM VALUES
          (0, CAST(1.5 AS DOUBLE)),
          (1, CAST(-2.5 AS DOUBLE)),
          (2, CAST(0.0 AS DOUBLE)),
          (3, CAST(NULL AS DOUBLE))
        AS t(id, v) ORDER BY id
        """
      Then query result
        | id | result |
        | 0  | -1.5   |
        | 1  | 2.5    |
        | 2  | -0.0   |
        | 3  | NULL   |

    Scenario Outline: Floating-point predicate: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                                                  | expr                                                      |
        | negate NaN is NaN by predicate                        | isnan(-CAST('NaN' AS DOUBLE))                             |
        | negate Infinity equals negative infinity by predicate | -CAST('Infinity' AS DOUBLE) = CAST('-Infinity' AS DOUBLE) |

  Rule: Day-time interval negation

    Scenario Outline: Interval: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                                           | expr                                                 |
        | negate a day-time interval                     | -(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND        |
        | negate a negative interval                     | -(INTERVAL '-3' DAY) = INTERVAL '3' DAY              |
        | negative function negates an interval          | negative(INTERVAL '5' SECOND) = INTERVAL '-5' SECOND |
        | double negation of an interval is the original | -(-(INTERVAL '5' SECOND)) = INTERVAL '5' SECOND      |

  Rule: String arguments coerce to double

    Scenario Outline: String coercion: <case>
      When query
        """
        SELECT negative(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | input | result |
        | negate a numeric string coerces to double | '1.5' | -1.5   |
        | negate a negative numeric string          | '-3'  | 3.0    |

    Scenario: negate a non-numeric string returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT negative('abc') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negate a non-numeric string errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT negative('abc') AS result
        """
      Then query error (?i)cast

  Rule: Negating the maximum decimal overflows its precision

    # Spark promotes `-DECIMAL(38,0)` to DECIMAL(39,0), which exceeds the max
    # precision (38), so negating the maximum value raises NUMERIC_VALUE_OUT_OF_RANGE.
    # This is a TYPE-precision overflow, so it errors regardless of ANSI mode (unlike
    # a value overflow, which only errors under ANSI on). Sail keeps DECIMAL(38,0)
    # and returns the value in both modes.
    @sail-bug
    Scenario Outline: Negating the maximum decimal overflows its precision: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT -CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS result
        """
      Then query error (?i)out.of.range

      Examples:
        | case                                                        | ansi  |
        | negate the maximum DECIMAL(38,0) errors under ANSI on       | true  |
        | negate the maximum DECIMAL(38,0) also errors under ANSI off | false |

  Rule: Signed numeric literals are typed before unary operations

    Scenario Outline: Signed numeric literal: <case>
      When query
        """
        SELECT CAST(<expr> AS STRING) AS value, typeof(<expr>) AS type
        """
      Then query result
        | value    | type   |
        | <value> | <type> |

      Examples:
        | case                                  | expr                           | value                | type          |
        | minimum tinyint                       | -128Y                          | -128                 | tinyint       |
        | minimum smallint                      | -32768S                        | -32768               | smallint      |
        | minimum int                           | -2147483648                    | -2147483648          | int           |
        | below minimum int widens to bigint    | -2147483649                    | -2147483649          | bigint        |
        | minimum bigint without suffix        | -9223372036854775808           | -9223372036854775808 | bigint        |
        | minimum bigint with suffix           | -9223372036854775808L          | -9223372036854775808 | bigint        |
        | below minimum bigint becomes decimal | -9223372036854775809           | -9223372036854775809 | decimal(19,0) |
        | parenthesized magnitude stays decimal | -(9223372036854775808)         | -9223372036854775808 | decimal(19,0) |
        | comments between sign and number     | - /* boundary */ 128Y         | -128                 | tinyint       |
        | unary plus around a negative literal | + -9223372036854775808L        | -9223372036854775808 | bigint        |
        | two minus operators                  | - -1L                         | 1                    | bigint        |
        | signed number before postfix cast    | -9223372036854775808L::BIGINT  | -9223372036854775808 | bigint        |
        | parenthesized number before cast     | (-9223372036854775808L)::BIGINT | -9223372036854775808 | bigint        |
        | number sign belongs inside the cast  | -1::STRING                    | -1                   | string        |
        | explicit unary minus stays outside   | -(1::STRING)                  | -1.0                 | double        |
        | negative double zero                 | -0.0D                         | -0.0                 | double        |
        | negative float zero                  | -0.0F                         | -0.0                 | float         |
        | negative exponent literal            | -1.25E+2                      | -125.0               | double        |
        | negative decimal exponent literal    | -.1234567E+2BD                | -12.34567            | decimal(7,5)  |

    Scenario Outline: Out-of-range signed literal: <expr>
      When query
        """
        SELECT <expr> AS value
        """
      Then query error (?i)<type>

      Examples:
        | expr                    | type     |
        | 128Y                    | tinyint  |
        | -129Y                   | tinyint  |
        | 32768S                  | smallint |
        | -32769S                 | smallint |
        | 9223372036854775808L     | bigint   |
        | -9223372036854775809L    | bigint   |
        | -(9223372036854775808L) | bigint   |

    Scenario: negating a signed minimum bigint literal overflows under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -(-9223372036854775808L) AS value
        """
      Then query error (?i)overflow

    Scenario: negating a signed minimum bigint literal wraps without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -(-9223372036854775808L) AS value
        """
      Then query result
        | value                |
        | -9223372036854775808 |

    @sail-only
    Scenario: a signed wide decimal literal preserves Sail's extra precision
      When query
        """
        SELECT CAST(-12345678901234567890123456789012345678.1BD AS STRING) AS value,
               typeof(-12345678901234567890123456789012345678.1BD) AS type
        """
      Then query result
        | value                                    | type          |
        | -12345678901234567890123456789012345678.1 | decimal(39,1) |

  Rule: Math functions coerce signed numeric strings

    Scenario Outline: Signed numeric strings retain each math function's result type
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result, typeof(<expr>) AS type
        """
      Then query result
        | result   | type   |
        | <result> | <type> |

      Examples:
        | ansi  | expr                                   | result | type          |
        | true  | bround(-1.25::STRING, 1)                | -1.2   | double        |
        | false | bround(-1.25::STRING, 1)                | -1.2   | double        |
        | true  | ceil(-1.25::STRING)                     | -1     | bigint        |
        | false | ceil(-1.25::STRING)                     | -1     | bigint        |
        | true  | floor(-1.25::STRING)                    | -2     | bigint        |
        | false | floor(-1.25::STRING)                    | -2     | bigint        |
        | true  | ceil(-1.25::STRING, 1)                  | -1.2   | decimal(22,1) |
        | false | ceil(-1.25::STRING, 1)                  | -1.2   | decimal(22,1) |
        | true  | floor(-1.25::STRING, 1)                 | -1.3   | decimal(22,1) |
        | false | floor(-1.25::STRING, 1)                 | -1.3   | decimal(22,1) |
        | true  | rint(-1.25::STRING)                     | -1.0   | double        |
        | false | rint(-1.25::STRING)                     | -1.0   | double        |
        | true  | hypot(-3::STRING, 4)                    | 5.0    | double        |
        | false | hypot(-3::STRING, 4)                    | 5.0    | double        |
        | true  | width_bucket(-1::STRING, -2, 2, 4)     | 2      | bigint        |
        | false | width_bucket(-1::STRING, -2, 2, 4)     | 2      | bigint        |
        | true  | width_bucket(1, 0, 2, -2::STRING)      | NULL   | bigint        |
        | false | width_bucket(1, 0, 2, -2::STRING)      | NULL   | bigint        |

    Scenario Outline: Unary plus coerces a signed string before concatenation
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (+ -7::STRING) || 'x' AS result
        """
      Then query result
        | result |
        | -7.0x  |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Math coercion applies to negative positive and NULL string columns
      When query
        """
        SELECT id, <expr> AS result
        FROM VALUES (0, '-1.25'), (1, '1.25'), (2, CAST(NULL AS STRING)) AS t(id, v)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 0  | <negative> |
        | 1  | <positive> |
        | 2  | NULL       |

      Examples:
        | expr                        | negative | positive |
        | bround(v, 1)                | -1.2     | 1.2      |
        | ceil(v)                     | -1       | 2        |
        | floor(v)                    | -2       | 1        |
        | ceil(v, 1)                  | -1.2     | 1.3      |
        | floor(v, 1)                 | -1.3     | 1.2      |
        | rint(v)                     | -1.0     | 1.0      |
        | hypot(v, 0)                 | 1.25     | 1.25     |
        | width_bucket(v, -2, 2, 4)   | 1        | 4        |
        | +v                          | -1.25    | 1.25     |
        | positive(v)                 | -1.25    | 1.25     |

    Scenario Outline: ANSI DIV promotes signed strings alongside integral operands
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <dividend> DIV <divisor> AS result,
               typeof(<dividend> DIV <divisor>) AS type
        """
      Then query result
        | result | type   |
        | -3     | bigint |

      Examples:
        | dividend   | divisor    |
        | -7::STRING | 2          |
        | 7          | -2::STRING |

    Scenario: ANSI DIV coerces string columns on either side
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, v DIV 2 AS dividend, 7 DIV v AS divisor
        FROM VALUES (0, '-7'), (1, '7'), (2, CAST(NULL AS STRING)) AS t(id, v)
        ORDER BY id
        """
      Then query result ordered
        | id | dividend | divisor |
        | 0  | -3       | -1      |
        | 1  | 3        | 1       |
        | 2  | NULL     | NULL    |

    Scenario: ANSI DIV rejects a fractional string when BIGINT is required
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT -7.5::STRING DIV 2 AS result
        """
      Then query error (?i)(cast|parse|convert)

    Scenario Outline: Malformed numeric strings become NULL outside ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result FROM VALUES ('bad') AS t(v)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | expr          |
        | positive(v)   |
        | ceil(v, 1)    |
        | bround(v, 1)  |

    Scenario Outline: Malformed numeric strings still fail under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result FROM VALUES ('bad') AS t(v)
        """
      Then query error (?i)(cast|parse|convert)

      Examples:
        | expr          |
        | positive(v)   |
        | ceil(v, 1)    |
        | bround(v, 1)  |

    Scenario: bround skips a malformed string when its scale is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT bround('bad', CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null integer literal yields a non-nullable integer
      When query
        """
        SELECT negative(5) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null integer column yields a non-nullable integer
      When query
        """
        SELECT negative(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable integer column stays nullable
      When query
        """
        SELECT negative(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
