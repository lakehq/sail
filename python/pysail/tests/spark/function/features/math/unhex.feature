Feature: unhex function

  # Spark's `Unhex` (`mathExpressions.scala`) reads the UTF-8 BYTES of its argument and decodes them
  # in pairs; `Hex.unhex` takes each digit through `java.util.HexFormat.fromHexDigit`, which accepts
  # only ASCII `[0-9A-Fa-f]` and throws for every other byte. With `failOnError = false` (what the
  # SQL `unhex` uses) a throw is a NULL, never an error. An odd number of digits is padded on the
  # LEFT (the first digit is a single nibble) and an empty string is an empty binary, not NULL.
  # `Unhex` is `ImplicitCastInputTypes` over STRING, `nullable = true`, and does not depend on ANSI.
  # Expected values measured on the Spark 4.2 JVM over Spark Connect.

  Rule: strings are decoded byte by byte

    Scenario Outline: unhex of a string: <case>
      When query template
        """
        SELECT hex(unhex(<input>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case          | input                    | result                 |
        | empty         | ''                       |                        |
        | zero          | '0'                      | 00                     |
        | 00            | '00'                     | 00                     |
        | f_lower       | 'f'                      | 0F                     |
        | F_upper       | 'F'                      | 0F                     |
        | ff            | 'ff'                     | FF                     |
        | FF            | 'FF'                     | FF                     |
        | Ff_mixed      | 'Ff'                     | FF                     |
        | odd3          | '123'                    | 0123                   |
        | ABC           | 'ABC'                    | 0ABC                   |
        | odd5          | 'abcde'                  | 0ABCDE                 |
        | prefix_0x     | '0x41'                   | NULL                   |
        | lead_space    | ' 41'                    | NULL                   |
        | trail_space   | '41 '                    | NULL                   |
        | inner_space   | '4 1'                    | NULL                   |
        | G1            | 'G1'                     | NULL                   |
        | 1G            | '1G'                     | NULL                   |
        | zz            | 'zz'                     | NULL                   |
        | plus          | '+1'                     | NULL                   |
        | minus         | '-1'                     | NULL                   |
        | dot           | '1.5'                    | NULL                   |
        | fullwidth     | '１２'                     | NULL                   |
        | e_acute       | 'é'                      | NULL                   |
        | e_acute_1     | 'é1'                     | NULL                   |
        | 41_e_acute    | '41é'                    | NULL                   |
        | emoji         | '😀'                      | NULL                   |
        | all_hex_chars | '0123456789abcdefABCDEF' | 0123456789ABCDEFABCDEF |
        | sentence      | 'Spark SQL'              | NULL                   |

    Scenario: a long string is decoded whole
      When query
        """
        SELECT length(unhex(repeat('AB', 20000))) AS even, length(unhex(concat(repeat('AB', 20000), 'C'))) AS odd
        """
      Then query result collected
        | even  | odd   |
        | 20000 | 20001 |

  # Anything that is not a STRING is cast to STRING first, so its printed text is what gets decoded.
  Rule: another type is decoded as its printed text

    Scenario Outline: unhex of <case>
      When query template
        """
        SELECT hex(unhex(<input>)) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case          | input                          | result       |
        | int           | 12                             | 12           |
        | int 255       | 255                            | 0255         |
        | int neg       | -5                             | NULL         |
        | int min       | CAST(-2147483648 AS INT)       | NULL         |
        | bigint        | 123456789012L                  | 123456789012 |
        | tinyint       | CAST(12 AS TINYINT)            | 12           |
        | double        | 1.5D                           | NULL         |
        | double whole  | 12.0D                          | NULL         |
        | double big    | 1.0E10D                        | NULL         |
        | float         | CAST(12.5 AS FLOAT)            | NULL         |
        | dec int       | CAST(12 AS DECIMAL(5,0))       | 12           |
        | dec frac      | CAST(12.5 AS DECIMAL(5,1))     | NULL         |
        | bool true     | true                           | NULL         |
        | bool false    | false                          | NULL         |
        | binary        | X'4142'                        | AB           |
        | binary empty  | X''                            |              |
        | binary nonhex | X'FF00'                        | NULL         |
        | date          | DATE'2024-01-02'               | NULL         |
        | timestamp     | TIMESTAMP'2024-01-02 03:04:05' | NULL         |
        | interval      | INTERVAL '5' DAY               | NULL         |
        | nan           | CAST('NaN' AS DOUBLE)          | NULL         |
        | untyped null  | NULL                           | NULL         |
        | null int      | CAST(NULL AS INT)              | NULL         |
        | null str      | CAST(NULL AS STRING)           | NULL         |
        | null bin      | CAST(NULL AS BINARY)           | NULL         |

  Rule: a type Spark cannot cast to STRING is rejected

    Scenario Outline: unhex of <case> is rejected
      When query template
        """
        SELECT unhex(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "unhex

      Examples:
        | case     | input               |
        | an array | array(1,2)          |
        | a map    | map('a',1)          |
        | a struct | named_struct('a',1) |

  # `TypeCoercion.scala:234`: a calendar interval is cast to STRING only when ANSI is on (the
  # year-month and day-time intervals always are). Its text is never hex, so unhex is NULL.
  Rule: a calendar interval needs ANSI to be cast to STRING

    Scenario: ANSI on: unhex of a calendar interval is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT unhex(make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query result collected
        | result |
        | NULL   |

    Scenario: ANSI off: unhex of a calendar interval is rejected
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT unhex(make_interval(0, 1, 0, 2, 0, 0, 0)) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE\] Cannot resolve "unhex.*(The first parameter|Parameter 1) requires the "STRING" type

    Scenario Outline: ANSI <ansi>: unhex of a day-time interval is NULL
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT unhex(INTERVAL '5' DAY) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

  # A VARIANT is cast to STRING by `VariantGet.cast`: a string is its raw text and a number its digits.
  Rule: a VARIANT is cast to its text

    @spark-4
    Scenario Outline: unhex of a VARIANT <case>
      When query template
        """
        SELECT hex(unhex(parse_json('<json>'))) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case   | json   | result |
        | string | "ab12" | AB12   |

    @spark-4
    Scenario: unhex of a number VARIANT
      When query
        """
        SELECT hex(unhex(parse_json('12'))) AS result
        """
      Then query result collected
        | result |
        | 12     |

  # A filter, a limit or an offset hands the function a slice of the batch (see `hex.feature`).
  Rule: a column that has been sliced or reduced to NULL

    Scenario: unhex of a column sliced with an offset
      When query
        """
        SELECT hex(unhex(c)) AS result
        FROM (SELECT c FROM VALUES ('41'), ('ZZ'), (CAST(NULL AS STRING)), ('4142') AS t(c) LIMIT 3 OFFSET 1)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | 4142   |

    Scenario: unhex of a column that is all NULL
      When query
        """
        SELECT hex(unhex(c)) AS result
        FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: unhex takes exactly one argument

    Scenario Outline: unhex with <case>
      When query template
        """
        SELECT <call> AS result
        """
      Then query error \[WRONG_NUM_ARGS\.WITHOUT_SUGGESTION\] The `unhex` requires 1 parameters

      Examples:
        | case          | call             |
        | no arguments  | unhex()          |
        | two arguments | unhex('41', 'x') |

  # Each row is decoded on its own: a bad row is NULL and does not touch its neighbours.
  Rule: a column is resolved per row

    Scenario: valid, invalid, NULL, empty, odd and half-invalid rows
      When query
        """
        SELECT hex(unhex(c)) AS result FROM VALUES ('41'), ('ZZ'), (NULL), (''), ('abc'), ('4G') AS t(c)
        """
      Then query result collected ordered
        | result |
        | 41     |
        | NULL   |
        | NULL   |
        |        |
        | 0ABC   |
        | NULL   |

    Scenario: a column that is all invalid
      When query
        """
        SELECT hex(unhex(c)) AS result FROM VALUES ('ZZ'), ('G'), ('xyz') AS t(c)
        """
      Then query result collected ordered
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: a column that is all NULL
      When query
        """
        SELECT unhex(CAST(c AS STRING)) AS result FROM VALUES (NULL), (NULL) AS t(c)
        """
      Then query result collected
        | result |
        | NULL   |
        | NULL   |

    Scenario: a column cut by a filter keeps its rows apart
      When query
        """
        SELECT hex(unhex(c)) AS result FROM (SELECT c FROM VALUES ('41'), ('ZZ'), ('4142'), (NULL), ('4344') AS t(c)) WHERE c IS NULL OR c != '41'
        """
      Then query result collected ordered
        | result |
        | NULL   |
        | 4142   |
        | NULL   |
        | 4344   |

  # A branch that is never taken is never evaluated, so an invalid value there does not matter.
  Rule: unhex keeps its place in a larger expression

    Scenario: an unused branch is not decoded
      When query
        """
        SELECT hex(IF(false, unhex('ZZ'), unhex('41'))) AS pick, CASE WHEN id < 100 THEN hex(unhex('41')) ELSE hex(unhex('ZZ')) END AS cased FROM range(1)
        """
      Then query result collected
        | pick | cased |
        | 41   | 41    |

    Scenario: unhex is usable in a predicate, a grouping and an ordering
      When query
        """
        SELECT hex(unhex(c)) AS h, count(*) AS n FROM VALUES ('41'), ('41'), ('ZZ') AS t(c) GROUP BY unhex(c) ORDER BY h
        """
      Then query result collected ordered
        | h    | n |
        | NULL | 1 |
        | 41   | 2 |

  Rule: hex and unhex are inverses

    Scenario: unhex of hex returns the bytes
      When query
        """
        SELECT unhex(hex(b)) = b AS result FROM VALUES (X'00FF10'), (X''), (X'7F80') AS t(b)
        """
      Then query result collected
        | result |
        | true   |
        | true   |
        | true   |

    Scenario: hex of unhex upper-cases and pads an odd count on the left
      When query
        """
        SELECT hex(unhex('abc')) AS result
        """
      Then query result collected
        | result |
        | 0ABC   |

    Scenario: every byte value survives the round trip
      When query
        """
        SELECT hex(unhex(h)) = upper(h) AS result FROM (SELECT concat_ws('', transform(sequence(0, 255), i -> lpad(hex(i), 2, '0'))) AS h)
        """
      Then query result collected
        | result |
        | true   |

  @function(nullability)
  Rule: Output schema

    # `Unhex.nullable` is `true` whatever the input (`mathExpressions.scala`), and the type is BINARY.
    Scenario: a non-null literal input to unhex yields the schema Spark declares
      When query
        """
        SELECT unhex('41') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column input to unhex yields the schema Spark declares
      When query
        """
        SELECT unhex(CAST(id AS STRING)) AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to unhex stays nullable
      When query
        """
        SELECT unhex(c) AS result FROM VALUES ('41'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: decode over unhex declares a nullable string
      When query
        """
        SELECT decode(unhex('537061726B2053514C'), 'UTF-8') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
