Feature: to_binary

  # `ToBinary` (`stringExpressions.scala`) is `RuntimeReplaceable`: with the `hex` format it IS
  # `Unhex(expr, failOnError = true)`, so a malformed value is an error (`CONVERSION_INVALID_INPUT`,
  # raised by `invalidInputInConversionError`), not a NULL. `try_to_binary` is the same expression
  # with `nullOnInvalidFormat = true` under `TryEval`, so it turns both a bad value and a bad
  # format into NULL. Measured on the Spark 4.2 JVM over Spark Connect.
  Rule: the hex format raises on a malformed value

    Scenario Outline: to_binary of a malformed value: <call>
      When query template
        """
        SELECT to_binary(<call>) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value '<value>' \('HEX'\) cannot be converted to "BINARY" because it is malformed

      Examples:
        | call          | value |
        | 'ZZ'          | ZZ    |
        | 'ZZ', 'hex'   | ZZ    |
        | 'ZZ', 'HEX'   | ZZ    |
        | 'QUJD', 'Hex' | QUJD  |
        | 'a!', 'hex'   | a!    |
        | '4G', 'hex'   | 4G    |

    Scenario Outline: to_binary of a well-formed value: <call>
      When query template
        """
        SELECT hex(to_binary(<call>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | call         | result |
        | '41'         | 41     |
        | '41', 'hex'  | 41     |
        | 'abc', 'HEX' | 0ABC   |
        | '', 'hex'    |        |
        | NULL, 'hex'  | NULL   |

    Scenario: one malformed row fails the whole query
      When query
        """
        SELECT to_binary(c, 'hex') AS result FROM VALUES ('41'), ('ZZ') AS t(c)
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value 'ZZ' \('HEX'\)

    Scenario: well-formed rows and NULLs are decoded one by one
      When query
        """
        SELECT hex(to_binary(c, 'hex')) AS result FROM VALUES ('41'), (NULL), ('4142') AS t(c)
        """
      Then query result collected ordered
        | result |
        | 41     |
        | NULL   |
        | 4142   |

    Scenario Outline: try_to_binary of a malformed value is NULL: <call>
      When query template
        """
        SELECT try_to_binary(<call>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | call        |
        | 'ZZ'        |
        | 'ZZ', 'hex' |
        | 'a!', 'HEX' |

  # The data is cast to STRING first, as `ImplicitCastInputTypes` says.
  Rule: the value is cast to STRING

    Scenario Outline: to_binary of <case>
      When query template
        """
        SELECT hex(to_binary(<input>, 'hex')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case   | input   | result |
        | an INT | 12      | 12     |
        | BINARY | X'4142' | AB     |

  # `ToBinary.checkInputDataTypes`: the format must be a foldable STRING (or NULL) naming one of
  # 'hex', 'utf-8', 'utf8', 'base64' compared with `toLowerCase(Locale.ROOT)` and NO trimming.
  # An unknown name is `INVALID_ARG_VALUE` for to_binary and NULL for try_to_binary; a NULL
  # format is NULL; a format that is not foldable is `NON_FOLDABLE_INPUT` for BOTH.
  Rule: the format argument is validated

    Scenario Outline: to_binary with a format that is not a format name: <case>
      When query template
        """
        SELECT to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*The fmt value must to be a case-insensitive "STRING" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got '<shown>'

      Examples:
        | case            | fmt       | shown   |
        | an unknown name | 'invalid' | invalid |
        | empty           | ''        |         |
        | an INT          | 1         | 1       |
        | a BOOLEAN       | true      | true    |

    Scenario: to_binary with a format that is not a format name: trailing space
      When query
        """
        SELECT to_binary('41', 'hex ') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*The fmt value must to be a case-insensitive "STRING" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got 'hex '

    Scenario: to_binary with a format that is not a format name: leading space
      When query
        """
        SELECT to_binary('41', ' hex') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*The fmt value must to be a case-insensitive "STRING" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got ' hex'

    Scenario: to_binary with a format that is not a format name: utf-8 with a trailing space
      When query
        """
        SELECT to_binary('41', 'utf-8 ') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*The fmt value must to be a case-insensitive "STRING" literal of 'hex', 'utf-8', 'utf8', or 'base64', but got 'utf-8 '

    Scenario Outline: try_to_binary with a format that is not a format name is NULL: <case>
      When query template
        """
        SELECT try_to_binary('41', <fmt>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case                        | fmt       |
        | trailing space              | 'hex '    |
        | leading space               | ' hex'    |
        | utf-8 with a trailing space | 'utf-8 '  |
        | an unknown name             | 'invalid' |
        | empty                       | ''        |
        | an INT                      | 1         |
        | a BOOLEAN                   | true      |

    Scenario Outline: a NULL format gives NULL: <function>, <case>
      When query template
        """
        SELECT <function>('41', <fmt>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | function      | case          | fmt                  |
        | to_binary     | NULL          | NULL                 |
        | to_binary     | a NULL STRING | CAST(NULL AS STRING) |
        | try_to_binary | NULL          | NULL                 |
        | try_to_binary | a NULL STRING | CAST(NULL AS STRING) |

    Scenario Outline: a foldable expression is accepted as the format: <case>
      When query template
        """
        SELECT hex(to_binary('41', <fmt>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case          | fmt                                        | result |
        | lower upper   | lower('HEX')                               | 41     |
        | upper lower   | upper('hex')                               | 41     |
        | concat        | concat('he', 'x')                          | 41     |
        | substring     | substring('xhex', 2)                       | 41     |
        | trim spaces   | trim(' hex ')                              | 41     |
        | case const    | CASE WHEN true THEN 'hex' ELSE 'utf-8' END | 41     |
        | if const      | IF(1 = 1, 'utf-8', 'base64')               | 3431   |
        | coalesce null | coalesce(CAST(NULL AS STRING), 'hex')      | 41     |
        | cast lit      | CAST('hex' AS STRING)                      | 41     |

    Scenario Outline: a foldable hex format still raises on a malformed value: <case>
      When query template
        """
        SELECT to_binary('ZZ', <fmt>) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\]

      Examples:
        | case          | fmt                                        |
        | lower upper   | lower('HEX')                               |
        | upper lower   | upper('hex')                               |
        | concat        | concat('he', 'x')                          |
        | substring     | substring('xhex', 2)                       |
        | trim spaces   | trim(' hex ')                              |
        | case const    | CASE WHEN true THEN 'hex' ELSE 'utf-8' END |
        | coalesce null | coalesce(CAST(NULL AS STRING), 'hex')      |
        | cast lit      | CAST('hex' AS STRING)                      |

    Scenario Outline: a foldable expression that is not a format name is rejected: <case>
      When query template
        """
        SELECT to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\]

      Examples:
        | case                      | fmt               |
        | concat to an unknown name | concat('he', 'y') |
        | lower of an unknown name  | lower('NOPE')     |
        | an INT expression         | 1 + 1             |
        | a BOOLEAN expression      | 1 = 1             |

    Scenario Outline: a format that is not foldable is rejected by both: <function>, <case>
      When query template
        """
        SELECT <function>('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.NON_FOLDABLE_INPUT\]

      Examples:
        | function      | case                           | fmt                                    |
        | to_binary     | a subquery                     | (SELECT 'hex')                         |
        | to_binary     | a volatile expression          | IF(rand() < 2, 'hex', 'utf-8')         |
        | to_binary     | a scalar subquery over a table | (SELECT f FROM VALUES ('hex') AS t(f)) |
        | try_to_binary | a subquery                     | (SELECT 'hex')                         |
        | try_to_binary | a volatile expression          | IF(rand() < 2, 'hex', 'utf-8')         |
        | try_to_binary | a scalar subquery over a table | (SELECT f FROM VALUES ('hex') AS t(f)) |

    Scenario Outline: a format taken from a column is rejected by both: <function>
      When query template
        """
        SELECT <function>('41', f) AS result FROM VALUES ('hex'), ('utf-8') AS t(f)
        """
      Then query error \[DATATYPE_MISMATCH\.NON_FOLDABLE_INPUT\]

      Examples:
        | function      |
        | to_binary     |
        | try_to_binary |

  # With the `utf-8` format the value is cast to STRING (`ImplicitCastInputTypes`, an AtomicType goes
  # to STRING in `TypeCoercion.scala:234`) and then `Encode`d, so the bytes are the printed text.
  Rule: utf-8 encodes the printed text of a non-string value

    Scenario Outline: to_binary utf-8 of <case>
      When query template
        """
        SELECT hex(to_binary(<input>, 'utf-8')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                | input                              | result                                                   |
        | BOOLEAN true        | true                               | 74727565                                                 |
        | BOOLEAN false       | false                              | 66616C7365                                               |
        | DOUBLE              | 1.5D                               | 312E35                                                   |
        | FLOAT               | CAST(1.5 AS FLOAT)                 | 312E35                                                   |
        | DECIMAL with scale  | 1.50BD                             | 312E3530                                                 |
        | DATE                | DATE'2024-01-02'                   | 323032342D30312D3032                                     |
        | TIMESTAMP           | TIMESTAMP'2024-03-05 06:07:08'     | 323032342D30332D30352030363A30373A3038                   |
        | TIMESTAMP_NTZ       | TIMESTAMP_NTZ'2024-03-05 06:07:08' | 323032342D30332D30352030363A30373A3038                   |
        | day-time interval   | INTERVAL '5' DAY                   | 494E54455256414C2027352720444159                         |
        | year-month interval | INTERVAL '1-2' YEAR TO MONTH       | 494E54455256414C2027312D3227205945415220544F204D4F4E5448 |

    Scenario Outline: to_binary utf-8 of <case> that already worked
      When query template
        """
        SELECT hex(to_binary(<input>, '<format>')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                 | input                    | format | result |
        | an INT               | 12                       | utf-8  | 3132   |
        | a BIGINT             | 12L                      | utf8   | 3132   |
        | a DECIMAL of scale 0 | CAST(12 AS DECIMAL(5,0)) | utf-8  | 3132   |
        | BINARY               | X'4142'                  | utf-8  | 4142   |
        | an empty string      | ''                       | utf-8  |        |

    Scenario: to_binary utf-8 over a DOUBLE column keeps NULL and the sign of zero
      When query
        """
        SELECT hex(to_binary(v, 'utf-8')) AS result
        FROM VALUES (1, 1.5D), (2, CAST(NULL AS DOUBLE)), (3, -0.0D) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result   |
        | 312E35   |
        | NULL     |
        | 2D302E30 |

    Scenario: to_binary utf-8 over a BOOLEAN column keeps NULL
      When query
        """
        SELECT hex(to_binary(v, 'utf-8')) AS result
        FROM VALUES (1, true), (2, false), (3, CAST(NULL AS BOOLEAN)) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 74727565   |
        | 66616C7365 |
        | NULL       |

    Scenario: to_binary utf-8 over a DATE column keeps NULL
      When query
        """
        SELECT hex(to_binary(v, 'utf-8')) AS result
        FROM VALUES (1, DATE'2024-01-02'), (2, CAST(NULL AS DATE)) AS t(i, v) ORDER BY i
        """
      Then query result ordered
        | result               |
        | 323032342D30312D3032 |
        | NULL                 |

  # The strict hex decoder reports the value it could not decode: the TEXT the value prints as.
  Rule: the error of a malformed non-string value names the printed text

    Scenario Outline: to_binary of <case> names its text
      When query template
        """
        SELECT to_binary(<input>) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value '<shown>' \('HEX'\) cannot be converted to "BINARY" because it is malformed

      Examples:
        | case      | input                          | shown               |
        | a DOUBLE  | 1.5D                           | 1.5                 |
        | a BOOLEAN | true                           | true                |
        | a DATE    | DATE'2024-01-02'               | 2024-01-02          |
        | TIMESTAMP | TIMESTAMP'2024-03-05 06:07:08' | 2024-03-05 06:07:08 |
        | a DECIMAL | 1.50BD                         | 1.50                |

    Scenario: to_binary of a day-time interval names its text
      When query
        """
        SELECT to_binary(INTERVAL '5' DAY) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value 'INTERVAL .{1,2}5.{1,2} DAY' \('HEX'\)

    Scenario: to_binary of a DOUBLE column names the first bad row's text
      When query
        """
        SELECT to_binary(v) AS result FROM VALUES (1.5D), (CAST(NULL AS DOUBLE)) AS t(v)
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value '1\.5' \('HEX'\)

  # A format that is a foldable BINARY (or any atomic type) is cast to STRING first; only an
  # ARRAY, MAP or STRUCT is a type error. The name is shown lower-cased, as `fmt` is.
  Rule: the format is cast to STRING

    Scenario Outline: to_binary with a BINARY format: <case>
      When query template
        """
        SELECT hex(to_binary(<input>, <fmt>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case           | input | fmt           | result |
        | hex as bytes   | '41'  | X'686578'     | 41     |
        | utf-8 as bytes | 'A'   | X'7574662D38' | 41     |

    Scenario: to_binary with a BINARY format that is not a name shows the text
      When query
        """
        SELECT to_binary('41', X'626164') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'bad'

    Scenario: to_binary with a DECIMAL format shows its text
      When query
        """
        SELECT to_binary('41', 1.5BD) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '1\.5'

    Scenario Outline: to_binary with a <case> format is a type error
      When query template
        """
        SELECT to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got <shown>

      Examples:
        | case   | fmt                 | shown        |
        | ARRAY  | array('hex')        | ARRAY        |
        | MAP    | map('a', 'b')       | MAP          |
        | STRUCT | named_struct('a',1) | NAMED_STRUCT |

    Scenario Outline: to_binary shows the lower-cased name: <fmt>
      When query template
        """
        SELECT to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '<shown>'

      Examples:
        | fmt     | shown |
        | 'BOGUS' | bogus |
        | 'HEX!'  | hex!  |

    Scenario: to_binary shows the lower-cased name: a trailing space is kept
      When query
        """
        SELECT to_binary('41', 'HeX ') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'hex '

  # `TypeCoercion.scala:234`: a calendar interval is not an AtomicType, so it is cast to STRING
  # only when ANSI is on; the year-month and day-time intervals are always cast.
  Rule: a calendar interval needs ANSI to be cast to STRING

    Scenario: ANSI on: to_binary utf-8 encodes the printed text
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(to_binary(make_interval(0, 1, 0, 2, 0, 0, 0), 'utf-8')) AS result
        """
      Then query result collected
        | result                         |
        | 31206D6F6E74687320322064617973 |

    Scenario: ANSI on: to_binary hex names the printed text
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_binary(make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value '1 months 2 days' \('HEX'\)

    Scenario Outline: ANSI off: <call> rejects it
      Given config spark.sql.ansi.enabled = false
      When query template
        """
        SELECT <call> AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "<name>.*(The first parameter|Parameter 1) requires the "STRING" type

      Examples:
        | call                                                   | name      |
        | to_binary(make_interval(0, 1, 0, 2, 0, 0, 0))          | to_binary |
        | to_binary(make_interval(0, 1, 0, 2, 0, 0, 0), 'utf-8') | to_binary |
        | try_to_binary(make_interval(0, 1, 0, 2, 0, 0, 0))      | to_binary |

  # `ToBinary.replacement` is `UnBase64(expr, failOnError = true)`, and `UnBase64.isValidBase64`
  # (`stringExpressions.scala`) accepts only well-formed input: the alphabet, `=` padding only at the
  # end and matching the length, whitespace skipped. A malformed value is `CONVERSION_INVALID_INPUT`.
  Rule: the base64 format is strict

    Scenario: to_binary base64 of a well-formed value
      When query
        """
        SELECT hex(to_binary('YWJj', 'base64')) AS result
        """
      Then query result collected
        | result |
        | 616263 |

    Scenario Outline: to_binary base64 of a malformed value: <value>
      When query template
        """
        SELECT hex(to_binary('<value>', 'base64')) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value '<shown>' \('BASE64'\) cannot be converted to "BINARY" because it is malformed

      Examples:
        | value   | shown   |
        | abc?    | abc.    |
        | abcdAE= | abcdAE= |
        | abcd=   | abcd=   |
        | ab==f   | ab==f   |
        | YW!Jj   | YW.Jj   |

    Scenario Outline: to_binary base64 of a <case>
      When query template
        """
        SELECT hex(to_binary(<input>, 'base64')) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case            | input                  | result |
        | BINARY          | CAST('YQ==' AS BINARY) | 61     |
        | BINARY of a pad | X'61673D3D'            | 6A     |

    Scenario: to_binary base64 of a BOOLEAN is its text decoded
      When query
        """
        SELECT hex(to_binary(true, 'base64')) AS result
        """
      Then query result collected
        | result |
        | B6BB9E |

  # `UnBase64` is `UnaryExpression`: its nullability is the child's. Only the hex (`Unhex`) and the
  # utf-8 (`Encode`) formats and `TryEval` are always nullable.
  @function(nullability)
  Rule: the base64 format keeps the nullability of its input

    Scenario: a non-null literal to_binary base64 is not nullable
      When query
        """
        SELECT to_binary('YWJj', 'base64') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

    Scenario: a non-null column to_binary base64 is not nullable
      When query
        """
        SELECT to_binary(CAST(id AS STRING), 'base64') AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

  # `Encode.encode` re-encodes a BINARY that is not valid UTF-8 through the charset, so the bytes
  # that are not UTF-8 become U+FFFD.
  Rule: utf-8 of an invalid BINARY replaces what is not UTF-8

    Scenario: to_binary utf-8 of an invalid byte
      When query
        """
        SELECT hex(to_binary(X'FF', 'utf-8')) AS result
        """
      Then query result collected
        | result |
        | EFBFBD |

  # Spark checks the format when it ANALYZES the call, so a branch that never runs and a plan that
  # only asks for a schema fail all the same.
  Rule: a bad format is rejected at analysis

    Scenario: a bad format in a branch that is never taken
      When query
        """
        SELECT IF(false, to_binary('41', 'bad'), X'00') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'bad'

    Scenario: a bad format over an empty relation
      When query
        """
        SELECT to_binary('41', 'bad') AS result FROM range(3) WHERE 1 = 0
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'bad'

  # A VARIANT is cast to STRING by `VariantGet.cast`: a string is its raw text, no quotes.
  Rule: a VARIANT is cast to its text

    @spark-4
    Scenario: to_binary of a string VARIANT
      When query
        """
        SELECT hex(to_binary(parse_json('"ab"'))) AS result
        """
      Then query result collected
        | result |
        | AB     |

  # The arms of `UnBase64.isValidBase64`: whitespace is skipped but not after the padding, a lone
  # trailing symbol is malformed, at most two `=` are allowed, and `Character.isWhitespace` does
  # not count the non-breaking space.
  Rule: the base64 validator follows every arm of isValidBase64

    Scenario: whitespace inside the groups is skipped
      When query
        """
        SELECT hex(to_binary('YW Jj', 'base64')) AS result
        """
      Then query result collected
        | result |
        | 616263 |

    Scenario Outline: to_binary base64 of a malformed value: <case>
      When query template
        """
        SELECT hex(to_binary(<input>, 'base64')) AS result
        """
      Then query error \[CONVERSION_INVALID_INPUT\] The value .* \('BASE64'\) cannot be converted to "BINARY"

      Examples:
        | case                  | input                           |
        | whitespace after pad  | 'YQ== '                         |
        | a lone last symbol    | 'abcde'                         |
        | three pad symbols     | 'YQ==='                         |
        | a non-breaking space  | concat('YWJj', chr(160))        |

  # A format that is not foldable is rejected when the call is analyzed, in the shapes where the call
  # never runs too. A foldable one (`concat('he', 'y')`, a CAST) is folded by the planner and checked
  # there, before the type of the value.
  Rule: a format that is not foldable is rejected at analysis

    Scenario Outline: to_binary with a <case> format
      When query template
        """
        <query>
        """
      Then query error \[DATATYPE_MISMATCH\.NON_FOLDABLE_INPUT\] Cannot resolve "to_binary

      Examples:
        | case                | query                                                                |
        | an ARRAY of columns | SELECT to_binary('a', array(c)) AS result FROM VALUES (1) AS t(c)    |
        | a column            | SELECT to_binary('41', c) AS result FROM VALUES ('hex') AS t(c)      |
        | a subquery in IF    | SELECT IF(false, to_binary('41', (SELECT 'hex')), X'00') AS result   |

    Scenario Outline: to_binary shows a non-string format as its text: <case>
      When query template
        """
        SELECT to_binary('41', <fmt>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '<shown>'

      Examples:
        | case   | fmt  | shown |
        | DOUBLE | 1.0D | 1.0   |

    Scenario: to_binary shows a FLOAT format as Java prints it
      When query
        """
        SELECT to_binary('41', CAST(1e10 AS FLOAT)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '1.0e10'

    Scenario: a foldable format that is not a literal, in a branch that never runs
      When query
        """
        SELECT IF(false, to_binary('41', concat('he', 'y')), X'00') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'hey'

    Scenario: the format is checked before the value type
      When query
        """
        SELECT to_binary(array(1), lower('BAD')) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got 'bad'

  # A TIMESTAMP is cast to STRING in the session time zone, as CAST does.
  Rule: a TIMESTAMP is encoded in the session time zone

    Scenario: a non-UTC session
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT hex(to_binary(TIMESTAMP '2024-03-05 06:07:08', 'utf-8')) AS a,
               hex(CAST(TIMESTAMP '2024-03-05 06:07:08' AS STRING)) AS b
        """
      Then query result collected
        | a                                      | b                                      |
        | 323032342D30332D30352030363A30373A3038 | 323032342D30332D30352030363A30373A3038 |

  # Without `try_`, an error raised while the argument is evaluated is not caught. Spark raises
  # `DIVIDE_BY_ZERO` and `CAST_OVERFLOW`; Sail raises the same errors with its own text, so the
  # pattern accepts both. The `try_to_binary` counterparts are in `try_to_binary.feature`.
  Rule: an error of the argument is raised

    Scenario: a division by zero in the argument
      When query
        """
        SELECT to_binary(CAST(1/0 AS STRING)) AS result
        """
      Then query error DIVIDE_BY_ZERO|[Dd]ivi(de|sion) by zero

    Scenario: an overflowing cast in the argument
      When query
        """
        SELECT to_binary(CAST(1.0E30D AS BIGINT), 'utf-8') AS result
        """
      Then query error CAST_OVERFLOW|Can't cast value

  # A format has to be foldable. A lambda variable is not, so the format cannot differ per element.
  Rule: a format that depends on a lambda variable is rejected at analysis

    @spark-4
    Scenario: the format is the lambda variable
      When query
        """
        SELECT transform(array('hex', 'utf-8'), f -> hex(to_binary('41', f))) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.NON_FOLDABLE_INPUT\] Cannot resolve "to_binary

  Rule: a foldable format is resolved like the literal it evaluates to

    # `UnBase64` follows its child's nullability, so a foldable `fmt` that evaluates to 'base64'
    # leaves the result non-nullable for a non-null input. The planner only sees a literal here.
    @sail-bug
    Scenario: a foldable base64 format that is not a literal
      When query
        """
        SELECT to_binary('YWJj', concat('base', '64')) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = false)
        """

    # The format is shown as the text Spark's cast to STRING prints, not as the raw value.
    Scenario: a calendar interval format is shown as Spark's cast to STRING
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_binary('41', make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '1 months 2 days'

    Scenario: a TIMESTAMP format is shown as its text
      When query
        """
        SELECT to_binary('a', TIMESTAMP '2020-01-01 00:00:00') AS result
        """
      Then query error \[DATATYPE_MISMATCH\.INVALID_ARG_VALUE\] Cannot resolve "to_binary.*but got '2020-01-01 00:00:00'

  # A filter, a limit or an offset hands the function a slice of the batch (see `hex.feature`). Spark
  # may push the projection below the limit, so a strict call is only compared where no row of the
  # relation is malformed; the slices that hold the malformed row must raise, and `try_` nulls it.
  Rule: a column that has been sliced or reduced to NULL

    Scenario Outline: to_binary <format> of a column sliced with an offset
      When query template
        """
        SELECT hex(to_binary(c, '<format>')) AS result
        FROM (SELECT c FROM VALUES (<a>), (<b>), (CAST(NULL AS STRING)), (<d>) AS t(c) LIMIT 2 OFFSET 2)
        """
      Then query result ordered
        | result |
        | NULL   |
        | <last> |

      Examples:
        | format | a      | b      | d        | last   |
        | hex    | '41'   | '42'   | '4142'   | 4142   |
        | utf-8  | 'a'    | 'bb'   | 'c'      | 63     |
        | base64 | 'YQ==' | 'Yg==' | 'YWJj'   | 616263 |

    Scenario Outline: to_binary <format> raises when the slice holds the malformed row
      When query template
        """
        SELECT hex(to_binary(c, '<format>')) AS result
        FROM (SELECT c FROM VALUES (<a>), (<b>), (CAST(NULL AS STRING)), (<d>) AS t(c) LIMIT 3 OFFSET 1)
        """
      Then query error \[CONVERSION_INVALID_INPUT\]

      Examples:
        | format | a      | b      | d        |
        | hex    | '41'   | 'ZZ'   | '4142'   |
        | base64 | 'YQ==' | 'a!'   | 'YWJj'   |

    Scenario Outline: try_to_binary <format> of a column sliced with an offset nulls only the malformed row
      When query template
        """
        SELECT hex(try_to_binary(c, '<format>')) AS result
        FROM (SELECT c FROM VALUES (<a>), (<b>), (CAST(NULL AS STRING)), (<d>) AS t(c) LIMIT 3 OFFSET 1)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | <last> |

      Examples:
        | format | a      | b      | d        | last   |
        | hex    | '41'   | 'ZZ'   | '4142'   | 4142   |
        | base64 | 'YQ==' | 'a!'   | 'YWJj'   | 616263 |

    Scenario Outline: to_binary <format> of a column that is all NULL
      When query template
        """
        SELECT hex(to_binary(c, '<format>')) AS result
        FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

      Examples:
        | format |
        | hex    |
        | utf-8  |
        | base64 |

  Rule: to_binary takes one or two arguments

    Scenario Outline: to_binary with <case>
      When query template
        """
        SELECT <call> AS result
        """
      Then query error \[WRONG_NUM_ARGS\.WITHOUT_SUGGESTION\] The `to_binary` requires \[1, 2\] parameters

      Examples:
        | case            | call                        |
        | no arguments    | to_binary()                 |
        | three arguments | to_binary('41', 'hex', 'x') |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary('abc', 'utf-8') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to to_binary yields the schema Spark declares
      When query
        """
        SELECT to_binary(CAST(id AS STRING), 'utf-8') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to to_binary stays nullable
      When query
        """
        SELECT to_binary(c, 'utf-8') AS result FROM VALUES ('abc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
